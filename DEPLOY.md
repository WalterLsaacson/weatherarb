# weatherarb 部署手册

本文描述在一台新 Linux 服务器上从零 clone、配置、验证并常驻运行 weatherarb 的完整流程。默认安装路径为 `/opt/weatherarb`；如果改用其他路径，必须同步修改 systemd unit 中的 `WorkingDirectory`、`ExecStart` 和日志路径。

部署完成后的效果：

- 常驻扫描 Polymarket 天气市场，提供看板与 API，默认监听 `0.0.0.0:8793`
- 从 Gamma 同步市场，从 Synoptic / NOAA 拉取观测，从 CLOB 拉取盘口
- 支持锁定桶限价单、日趋势 FAK、动态 0.999 退出、扫尾盘 dry-run/live
- 通过 systemd 开机自启，进程崩溃自动重启

## 1. 部署前检查

### 1.1 系统要求

| 项目 | 要求 |
|---|---|
| 系统 | Ubuntu 22.04 或更新版本，x86_64 |
| Python | 3.11+，live 下单依赖 `polymarket-client` |
| 内存 | 至少 2GB；启动后约 100-300MB，长跑需监控缓慢增长，接近 1.2GB 时安排重启 |
| 磁盘 | 至少 10GB；系统会自动清理部分滚动日志 |
| 网络 | 能访问 Gamma、CLOB、Open-Meteo、Synoptic / NOAA |

需要访问的外部服务：

| 用途 | 主机 |
|---|---|
| 市场目录与结算价格 | `gamma-api.polymarket.com` |
| 盘口与订单 | `clob.polymarket.com` |
| 全天天气预报 | `api.open-meteo.com` |
| 气象站观测 | `api.synopticdata.com`、`www.weather.gov` |

如服务器不能直连，可在 `.env` 配置 `WEATHER_PROXY` / `WEATHER_PROXY_FALLBACK` / `PM_PROXY`。

### 1.2 Live 安全规则

**绝对不要让两台机器同时使用同一个钱包 live 运行。** 否则会重复下单、重复挂单或重复管理同一持仓。

推荐迁移顺序：

1. 新机先以 `LIVE_ORDERS=false` 完成冒烟与验收
2. 停止旧机 live 服务
3. 迁移交易状态文件
4. 启动新机 live 服务
5. 确认新机稳定后，旧机保持停止，不要再启动

`.env` 不进 Git，必须在新机手动创建。不要把私钥写入文档、issue、聊天记录或截图。

## 2. 安装系统依赖

```bash
sudo apt-get update
sudo apt-get install -y curl ca-certificates git build-essential
```

安装 `uv`，用它管理 Python 3.11 和虚拟环境：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

建议把 `export PATH="$HOME/.local/bin:$PATH"` 写入 `~/.bashrc`。

## 3. Clone 代码

HTTPS 方式：

```bash
sudo mkdir -p /opt
sudo git clone https://github.com/WalterLsaacson/weatherarb.git /opt/weatherarb
sudo chown -R "$USER:$USER" /opt/weatherarb
cd /opt/weatherarb
git checkout main
git pull --ff-only
```

SSH 方式：

```bash
sudo mkdir -p /opt
sudo git clone git@github.com:WalterLsaacson/weatherarb.git /opt/weatherarb
sudo chown -R "$USER:$USER" /opt/weatherarb
cd /opt/weatherarb
```

如果使用专用 SSH key，只配置在本仓库，避免修改全局 Git 身份：

```bash
git config core.sshCommand "ssh -i /path/to/deploy_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
```

## 4. 创建 Python 3.11 虚拟环境

```bash
cd /opt/weatherarb
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements-live.txt
```

验证：

```bash
.venv/bin/python --version
.venv/bin/python -c "from polymarket.clients.secure import SecureClient; print('SDK OK', SecureClient)"
```

如果只部署看板 / dry-run，理论上可以不安装 `requirements-live.txt`，但为了运维一致性，建议始终安装。

## 5. 运行测试

```bash
cd /opt/weatherarb
.venv/bin/python -m unittest discover -s tests
```

预期最后一行是 `OK`。测试过程中可能打印 `LIVE auto-take`、`LIMIT LIVE`、`EXIT SELL` 等内容，这些来自 fixtures 和 mock，不会访问交易所，也不是真实订单。

## 6. 配置 `.env`

```bash
cd /opt/weatherarb
cp deploy/env.example .env
chmod 600 .env
vi .env
```

必须填写：

| 变量 | 说明 |
|---|---|
| `PRIVATE_KEY` | Polymarket 签名私钥 |
| `FUNDER` | 出资 / proxy 钱包地址 |
| `SIGNATURE_TYPE` | 与钱包类型匹配；当前生产使用 `3` |
| `SYNOPTIC_API_TOKEN` | Synoptic / MesoWest API token |

以下是与当前生产策略一致的推荐基线，不含密钥。第一次部署时建议先保持所有 live 开关为 `false`，完成在线 dry-run 后再开启：

```env
LIVE_ORDERS=true
MAX_ORDER_USDC=5
MIN_NET_EDGE=0.0075
MAX_ASK=0.995
MAX_SLIPPAGE=0.003

LIMIT_ORDERS=true
LIMIT_ORDER_USDC=5
LIMIT_ORDER_PRICE=0.99
LIMIT_ORDER_PRICE_001=0.995
LIMIT_ORDER_MIN_PRICE=0.01
LIMIT_ORDER_MAX_PRICE=0.99

DYNAMIC_EXIT=true
EXIT_PRICE_001=0.999
EXIT_MIN_SIZE=5
EXIT_INTERVAL_S=1800
EXIT_MAX_ORDERS_PER_CYCLE=10
EXIT_UNAVAILABLE_RETRY_S=3600

DIURNAL_ORDERS=true
DIURNAL_YES_ORDERS=true
DIURNAL_NO_ORDERS=true
DIURNAL_ORDER_USDC=5
DIURNAL_MIN_PRICE=0.90
DIURNAL_FORECAST_GATE=true
FORECAST_CACHE_TTL_S=3600

SWEEP_TAIL_ENABLED=true
SWEEP_TAIL_ORDERS=false
SWEEP_TAIL_ORDER_USDC=5
SWEEP_TAIL_LEADER_MIN=0.99
SWEEP_TAIL_LOSER_MAX=0.01
SWEEP_TAIL_MIN_LEGS=3
```

关键开关说明：

| 变量 | 行为 |
|---|---|
| `LIVE_ORDERS` | 所有真实 FAK / 自动下单的总开关 |
| `LIMIT_ORDERS` | 锁定桶 GTC 限价单 |
| `DYNAMIC_EXIT` | 买入成交后自动挂 0.999 GTC 卖出；且受 `LIVE_ORDERS` 控制 |
| `DIURNAL_ORDERS` | 日趋势策略 legacy 总开关 |
| `DIURNAL_YES_ORDERS` | 日趋势买当前挡 YES；未设置时沿用 `DIURNAL_ORDERS` |
| `DIURNAL_NO_ORDERS` | 日趋势买其他挡 NO |
| `DIURNAL_FORECAST_GATE` | 事件日与前一日全天天气类型必须一致，且预报温度必须覆盖当前运行值；最低温向下取整，最高温向上取整 |
| `SWEEP_TAIL_ENABLED` | 扫尾盘发现与记录 |
| `SWEEP_TAIL_ORDERS` | 扫尾盘真实下单独立开关；`false` 表示 dry-run |

修改 `.env` 后必须重启服务。

## 7. 迁移交易状态

如果这是全新钱包、全新数据集，可以跳过本节，直接创建数据目录：

```bash
cd /opt/weatherarb
mkdir -p data/pm-weather-live data/run
```

如果是从旧 live 机器迁移，**必须先停旧机**，再打包状态文件：

```bash
# 旧机
sudo systemctl stop weatherarb
cd /root/workspace/weatherarb/data/pm-weather-live
tar --ignore-failed-read -czf /tmp/weatherarb-state.tgz \
  orders.jsonl \
  exit_orders.jsonl \
  dynamic_exit.json \
  diurnal_seen.json \
  sweep_tail_opportunities.jsonl
```

把 `/tmp/weatherarb-state.tgz` 传到新机后：

```bash
# 新机
cd /opt/weatherarb
mkdir -p data/pm-weather-live data/run
tar -xzf /tmp/weatherarb-state.tgz -C data/pm-weather-live
chmod 600 data/pm-weather-live/dynamic_exit.json
```

文件用途：

| 文件 | 是否建议迁移 | 原因 |
|---|---|---|
| `orders.jsonl` | 必须 | 已提交 / 已成交 token 去重，避免重复买 |
| `exit_orders.jsonl` | 必须 | 动态退出历史，避免重复挂卖单 |
| `dynamic_exit.json` | 必须 | 当前卖单状态、成交量、closed 标记 |
| `diurnal_seen.json` | 强烈建议 | 日趋势已买 / 已见过状态 |
| `sweep_tail_opportunities.jsonl` | 建议 | 扫尾盘机会、执行审计、resolution 记录 |

以下文件**不需要**迁移，服务会自动重建：

- `latest.json`
- `market_snapshot.json`
- `rules.auto.json`
- `weather_catalog.json`
- `scan.jsonl`
- `source_observations.jsonl`
- `synoptic_watch.jsonl`
- `candidates.jsonl`
- `health.json`

不要直接打包整个 `data/` 目录。历史观测和扫尾日志可能非常大，不是部署必需品，还会拖慢启动并增加内存压力。

服务会每 36 小时自动清理或截断部分滚动日志，但长期保留的订单、去重、动态退出状态不会被清空。

## 8. 冒烟验证

### 8.1 Fixture 冒烟

不访问外部市场，也不下单：

```bash
cd /opt/weatherarb
mkdir -p /tmp/weatherarb-fixture data/run
LIVE_ORDERS=false LIMIT_ORDERS=false DYNAMIC_EXIT=false \
  .venv/bin/python run_weather.py \
  --fixture fixtures \
  --data-dir /tmp/weatherarb-fixture \
  --host 127.0.0.1 \
  --port 8793 \
  --no-open
```

另开一个终端：

```bash
curl -fsS http://127.0.0.1:8793/api/health
curl -fsS http://127.0.0.1:8793/api/status
```

确认后 `Ctrl+C` 结束进程。

### 8.2 在线 dry-run

代码读取 `.env` 时使用 `os.environ.setdefault`，因此命令行中显式传入的变量会覆盖 `.env`。以下命令即使 `.env` 里是 live，也会强制 dry-run：

```bash
cd /opt/weatherarb
mkdir -p /tmp/weatherarb-smoke
LIVE_ORDERS=false \
LIMIT_ORDERS=false \
DYNAMIC_EXIT=false \
DIURNAL_ORDERS=false \
DIURNAL_YES_ORDERS=false \
DIURNAL_NO_ORDERS=false \
SWEEP_TAIL_ORDERS=false \
  .venv/bin/python run_weather.py \
  --sync \
  --data-dir /tmp/weatherarb-smoke \
  --interval 30 \
  --host 127.0.0.1 \
  --port 8794 \
  --no-open
```

验收：

```bash
curl -fsS http://127.0.0.1:8794/api/health
curl -fsS http://127.0.0.1:8794/api/status
```

健康接口应返回 `"ok": true`。观察 `ticks` 和 `last_scan_at` 持续刷新。确认无异常后 `Ctrl+C`。

## 9. 安装 systemd 服务

默认路径为 `/opt/weatherarb`：

```bash
cd /opt/weatherarb
mkdir -p data/run
sudo cp deploy/weatherarb.service /etc/systemd/system/weatherarb.service
sudo systemctl daemon-reload
sudo systemctl enable --now weatherarb
systemctl is-active weatherarb
```

如果安装路径不是 `/opt/weatherarb`，先在仓库内生成一份修改路径后的 unit，再安装：

```bash
cd /path/to/weatherarb
cp deploy/weatherarb.service /tmp/weatherarb.service
sed -i "s#/opt/weatherarb#$PWD#g" /tmp/weatherarb.service
sudo cp /tmp/weatherarb.service /etc/systemd/system/weatherarb.service
rm /tmp/weatherarb.service
sudo systemctl daemon-reload
sudo systemctl enable --now weatherarb
```

常用管理命令：

```bash
sudo systemctl start weatherarb
sudo systemctl stop weatherarb
sudo systemctl restart weatherarb
systemctl status weatherarb --no-pager -l
journalctl -u weatherarb -n 200 --no-pager
tail -f data/run/weatherarb.log
```

生产启动参数：

```text
run_weather.py --sync --data-dir data/pm-weather-live --interval 30 --host 0.0.0.0 --port 8793 --no-open
```

## 10. 生产验收清单

依次确认：

```bash
systemctl is-active weatherarb
curl -fsS http://127.0.0.1:8793/api/health
curl -fsS http://127.0.0.1:8793/api/status
systemctl show weatherarb -p MainPID -p NRestarts -p MemoryCurrent -p Result
free -h
stat -c "%s %n" data/pm-weather-live/latest.json data/pm-weather-live/health.json
```

验收标准：

- [ ] `systemctl is-active weatherarb` 返回 `active`
- [ ] 健康接口 `"ok": true`
- [ ] `ticks` 增加，`last_scan_at` 刷新
- [ ] live 部署时，状态接口显示私钥和 funder 已配置、live 开关符合预期
- [ ] `NRestarts` 稳定，不因 OOM 反复增加
- [ ] 启动后内存在约 100-300MB 量级；长跑中若 RSS 接近 1.2GB，安排计划内重启
- [ ] `data/run/weatherarb.log` 无持续 traceback
- [ ] 旧机 `weatherarb` 保持 `inactive`

有真实机会时，日志可能出现：

```text
LIMIT LIVE ...
LIVE auto-take ...
EXIT SELL ... @ 0.999 ...
```

没有机会时不会出现这些日志；不要把“没有机会”判断为服务异常。

动态退出按 `EXIT_INTERVAL_S` 周期运行，默认 30 分钟。买入成交后不要期望立即出现卖单日志。

## 11. 升级流程

```bash
sudo systemctl stop weatherarb
cd /opt/weatherarb
git fetch origin
git pull --ff-only
uv pip install --python .venv/bin/python -r requirements-live.txt
.venv/bin/python -m unittest discover -s tests
sudo systemctl start weatherarb
systemctl is-active weatherarb
```

如果 `.env` 有变化：

```bash
vi .env
sudo systemctl restart weatherarb
```

## 12. 日志与磁盘维护

日常检查：

```bash
du -sh data/pm-weather-live
find data/pm-weather-live -maxdepth 1 -type f -printf "%s %f\n" | sort -nr | head -n 20
```

系统会自动处理大部分滚动日志。如果 `sweep_tail_opportunities.jsonl` 明显过大，可在停机窗口归档并只保留末尾：

```bash
sudo systemctl stop weatherarb
cd /opt/weatherarb/data/pm-weather-live
gzip -c sweep_tail_opportunities.jsonl > sweep_tail_opportunities.$(date -u +%Y%m%dT%H%M%SZ).jsonl.gz
tail -n 5000 sweep_tail_opportunities.jsonl > sweep_tail_opportunities.recent.jsonl
mv sweep_tail_opportunities.recent.jsonl sweep_tail_opportunities.jsonl
sudo systemctl start weatherarb
```

历史归档只用于复盘，不参与下单；如果磁盘紧张，可以转移到对象存储或下载到本地后删除服务器上的 gzip。

## 13. 安全建议

- `.env` 必须保持 `600`
- 看板端口不要无限制暴露公网；如需远程访问，用防火墙限制来源 IP 或通过 SSH tunnel
- 确认私钥只存在于一台 live 机器
- 迁移后确认旧机服务不会开机自启
- 定期备份订单、退出状态和去重状态
- 不要把 `data/` 或 `.env` 提交到 Git

## 14. 故障排查

### Python / SDK

```text
ModuleNotFoundError: polymarket
```

说明虚拟环境未安装 live 依赖，或用了系统 Python。执行：

```bash
uv pip install --python .venv/bin/python -r requirements-live.txt
```

### 服务无法启动

```bash
journalctl -u weatherarb -n 200 --no-pager
tail -n 200 data/run/weatherarb.log
```

常见原因：

- systemd unit 路径仍是 `/opt/weatherarb`
- `data/run` 不存在
- `.env` 权限或格式错误
- Python 虚拟环境路径错误

### 端口占用

```bash
ss -ltnp | grep 8793
```

先停止旧进程或改用其他端口。

### 网络超时

```bash
curl -I https://gamma-api.polymarket.com
curl -I https://clob.polymarket.com
```

确认网络、代理和 `SYNOPTIC_API_TOKEN`。如果部分服务超时，看板可能仍能启动，但不会有完整机会。

### 内存反复重启

```bash
journalctl -u weatherarb --since "1 hour ago" --no-pager | grep -Ei "oom|killed|memory"
free -h
systemctl show weatherarb -p NRestarts -p MemoryCurrent
```

当前版本长跑时 RSS 可能缓慢增长。不要等 OOM killer 触发；在无紧急持仓处理需求时执行计划内重启：

```bash
sudo systemctl restart weatherarb
systemctl show weatherarb -p MemoryCurrent
```

处理原则：

- 不要把旧机完整 `data/` 目录直接搬到新机
- 不要迁移巨大的 `latest.json` 和历史观测日志
- 确认使用包含盘口快照压缩和启动水合保护的代码版本
- 必要时归档并截断 `sweep_tail_opportunities.jsonl`

### 疑似重复下单

立即处理：

```bash
sudo systemctl stop weatherarb
journalctl -u weatherarb -n 200 --no-pager
```

确认是否两台机器同时 live，是否漏迁 `orders.jsonl` / `diurnal_seen.json`。交易所侧还需人工检查未成交订单并撤单。

### 配置未生效

`.env` 修改后必须：

```bash
sudo systemctl restart weatherarb
```

命令行变量优先级高于 `.env`，冒烟时传入的 `LIVE_ORDERS=false` 只对当次进程生效。
