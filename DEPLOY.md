# weatherarb 部署手册（迁移到新服务器）

目标：在新机器上 `clone` 后按本文部署，达到与当前生产机相同效果：

- 常驻扫描 + 看板（`0.0.0.0:8793`）
- Gamma sync + Synoptic/CLOB 只读拉取
- `LIVE_ORDERS=true` / `LIMIT_ORDERS=true` 时自动挂/撤限价单
- systemd 开机自启，日志落到 `data/run/weatherarb.log`

本文路径默认安装到 `/opt/weatherarb`。可改成任意目录，但 **systemd 里三处路径要一起改**。

---

## 0. 迁移注意（先读）

1. **不要两台机器同时用同一钱包 live 跑。** 否则会重复挂单。新机验证通过后，先停旧机再开新机 live。
2. `.env` **不会进 git**，必须从旧机手动拷贝。
3. 若希望新机继承「已挂过哪些 token」的去重状态，可额外拷贝旧机  
   `data/pm-weather-live/orders.jsonl`（可选；不拷贝会重新扫描后按当前锁重新挂）。
4. 系统建议：**Ubuntu 22.04+ x86_64**，能直连外网（或自行配代理）。
5. Live 下单需要 **Python 3.11+**（官方 `polymarket-client`）。系统自带 3.10 不够。

需要访问的公网（直连）：

| 用途 | 主机示例 |
|---|---|
| 市场目录 | `gamma-api.polymarket.com` |
| 盘口 / 下单 | `clob.polymarket.com` |
| 气象（NOAA Synoptic） | `api.synopticdata.com`、`www.weather.gov` |
| 看板端口 | TCP `8793`（按需对公网或仅内网开放） |

---

## 1. 安装系统依赖

```bash
sudo apt-get update
sudo apt-get install -y curl ca-certificates git build-essential
```

安装 `uv`（用来装 Python 3.11 和虚拟环境）：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
# 可写入 ~/.bashrc
uv --version
```

---

## 2. Clone 代码

```bash
sudo mkdir -p /opt
sudo git clone git@github.com:WalterLsaacson/weatherarb.git /opt/weatherarb
# 若用 HTTPS：
# sudo git clone https://github.com/WalterLsaacson/weatherarb.git /opt/weatherarb

sudo chown -R "$USER:$USER" /opt/weatherarb
cd /opt/weatherarb
git checkout main
git pull --ff-only
```

SSH 推拉若沿用本机那把部署密钥，可在**本仓库**设置（不要改全局 `user.*`）：

```bash
# 示例：密钥路径按新机实际位置修改
git config core.sshCommand 'ssh -i /root/.ssh/dqdhook-server -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new'
```

---

## 3. Python 3.11 虚拟环境 + live 依赖

```bash
cd /opt/weatherarb
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements-live.txt

.venv/bin/python --version
# 期望：Python 3.11.x
.venv/bin/python -c "from polymarket.clients.secure import SecureClient; print('SDK OK', SecureClient)"
```

只跑 dry-run / 看板、不下单时，可以跳过 `requirements-live.txt`，用系统 `python3` 也能起 fixture；**要与本机同样 live 效果则必须装 SDK。**

---

## 4. 配置 `.env`

```bash
cd /opt/weatherarb
cp deploy/env.example .env
chmod 600 .env
# 把旧机 .env 内容贴进来，或逐项填写
```

与本机对齐的关键字段：**私钥 / FUNDER / SYNOPTIC_API_TOKEN / LIVE_ORDERS / LIMIT_ORDERS 及限额**。示例见 `deploy/env.example`。

常用含义：

| 变量 | 说明 |
|---|---|
| `PRIVATE_KEY` | Polymarket 签名私钥 |
| `FUNDER` | 资金地址（proxy / 实际出资地址） |
| `SIGNATURE_TYPE` | 本机为 `3`（与账户类型一致） |
| `CHAIN_ID` | `137`（Polygon） |
| `LIVE_ORDERS` | `true` 才真实下单；否则 dry-run |
| `LIMIT_ORDERS` | `true` 时对锁定桶挂 GTC |
| `LIMIT_ORDER_PRICE` | 本机 `0.99` |
| `MAX_ORDER_USDC` | FAK 吃单名义本金上限 |
| `LIMIT_ORDER_USDC` | GTC 挂单目标名义本金；live 时实际为 `min(可用余额, LIMIT_ORDER_USDC)` |
| `SYNOPTIC_API_TOKEN` | Synoptic 气象 API token |
| `POLY_API_KEY` / `SECRET` / `PASSPHRASE` | 可留空；缺省时由 SDK 用私钥派生 |

改完 `.env` 后必须 `systemctl restart weatherarb`。

---

## 5. 数据目录（可选迁移）

```bash
mkdir -p /opt/weatherarb/data/run /opt/weatherarb/data/pm-weather-live
```

可选从旧机拷贝（停旧机 live 之后再拷更安全）：

```bash
# 在旧机打包
# tar czf weatherarb-data.tgz -C /root/workspace/weatherarb/data pm-weather-live

# 在新机
# tar xzf weatherarb-data.tgz -C /opt/weatherarb/data
```

至少建议拷贝：

- `orders.jsonl`：已提交 GTC / 去重状态
- 不强制：`latest.json`、`rules.auto.json`、`weather_catalog.json`（缺了会重新 sync）

进程会每 **36 小时**自动清理体积较大的 `scan.jsonl` / `source_observations.jsonl` 等滚动日志；`orders.jsonl` 会保留。

---

## 6. 先手动试跑（推荐）

**Fixture 冒烟（不下单、不访问外网市场）：**

```bash
cd /opt/weatherarb
.venv/bin/python run_weather.py --fixture fixtures --data-dir /tmp/weatherarb-fixture --host 127.0.0.1 --port 8793 --no-open
# 浏览器或 curl：http://127.0.0.1:8793/api/health
# Ctrl+C 结束
```

**与生产相同的在线命令（确认 `.env` 后再开）：**

```bash
cd /opt/weatherarb
.venv/bin/python run_weather.py \
  --sync \
  --data-dir data/pm-weather-live \
  --interval 30 \
  --host 0.0.0.0 \
  --port 8793 \
  --no-open
```

看到类似输出即正常：

```text
Weather scanner + board → http://0.0.0.0:8793/
Mode → LIVE auto-take · max 5.0 USDC · ...
```

若 `.env` 里 `LIVE_ORDERS` 不是 true，会显示 `Mode → dry-run/read-only`。

健康检查：

```bash
curl -sS http://127.0.0.1:8793/api/health
curl -sS http://127.0.0.1:8793/api/status | python3 -m json.tool | head
```

看板：`http://<新机IP>:8793/`

---

## 7. 安装 systemd 常驻（与本机一致）

```bash
cd /opt/weatherarb
# 若安装路径不是 /opt/weatherarb，先编辑 WorkingDirectory / ExecStart / 日志路径
sudo cp deploy/weatherarb.service /etc/systemd/system/weatherarb.service
sudo systemctl daemon-reload
sudo systemctl enable --now weatherarb.service
sudo systemctl status weatherarb.service --no-pager -l
```

常用命令：

```bash
sudo systemctl restart weatherarb
sudo systemctl stop weatherarb
journalctl -u weatherarb -n 100 --no-pager
tail -f /opt/weatherarb/data/run/weatherarb.log
```

生产启动参数（写死在 unit 里，勿漏）：

```text
run_weather.py --sync --data-dir data/pm-weather-live --interval 30 --host 0.0.0.0 --port 8793 --no-open
```

---

## 8. 验收清单（对齐本机效果）

- [ ] `systemctl is-active weatherarb` → `active`
- [ ] `curl http://127.0.0.1:8793/api/health` → `"ok": true`
- [ ] `/api/status` 中 `dry_run: false`，`trading.live_orders: true`，`limit_orders: true`
- [ ] `trading.has_private_key: true`，`has_funder: true`
- [ ] 一两分钟内 `ticks` 增加；`last_scan_at` 刷新；扫描总耗时通常约数十秒级（冷启动可能稍长）
- [ ] 日志出现 `LIMIT LIVE ...` / `LIMIT CANCEL LIVE ...`（有锁定桶时）
- [ ] 旧机已 `systemctl stop weatherarb`（避免双开）

---

## 9. 防火墙 / 安全

```bash
# 若用 ufw，仅示例：开放看板（按需收紧来源 IP）
# sudo ufw allow 8793/tcp
```

建议：

- `.env` 权限 `600`，不要提交到 git
- 看板默认只读 UI，但公网暴露仍建议限制来源 IP
- 迁移完成后轮换或确认私钥只在一台 live 机器上使用

---

## 10. 故障排查

| 现象 | 处理 |
|---|---|
| `ModuleNotFoundError: polymarket` | 确认用的是 `.venv/bin/python`，且已 `uv pip install -r requirements-live.txt` |
| `Mode → dry-run` | 检查 `.env` 的 `LIVE_ORDERS=true` 后 restart |
| 气象阶段极慢 / 大量 unavailable | 检查 `SYNOPTIC_API_TOKEN`、到 `api.synopticdata.com` 的连通性 |
| 端口起不来 | `ss -ltnp \| grep 8793`；改 unit 里 `--port` |
| 重复挂单 | 是否旧机仍在 live；或未带上旧 `orders.jsonl` 导致去重集为空（会重新挂当前锁） |
| 服务起不来 | `tail -100 data/run/weatherarb.log` 与 `journalctl -u weatherarb -e` |

---

## 11. 本机对照摘要

| 项 | 本机当前值 |
|---|---|
| 代码目录 | `/root/workspace/weatherarb`（新机建议 `/opt/weatherarb`） |
| 解释器 | `.venv` + Python 3.11 + `polymarket-client==0.10.0` |
| 端口 | `8793` |
| 扫描间隔 | `30s` |
| 数据目录 | `data/pm-weather-live` |
| 日志 | `data/run/weatherarb.log` |
| unit 名 | `weatherarb.service` |
| live | `LIVE_ORDERS=true`，`LIMIT_ORDERS=true`，限价 `0.99`，单笔 `5` USDC |

完成以上步骤后，新机行为应与本机一致：持续扫描天气终局、看板可访问，并在锁定桶上自动维护 GTC 限价单。
