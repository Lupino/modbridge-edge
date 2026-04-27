# ModBridge Edge

MQTT <-> Modbus bridge for ESP32/4G DTU devices.
面向 ESP32/4G DTU 设备的 MQTT 与 Modbus 双向桥接服务。

It receives cloud requests from MQTT, forwards raw Modbus frames to DTU, then parses responses and publishes structured results back to MQTT topics.
服务从 MQTT 接收云端请求，将 Modbus 原始帧转发给 DTU，并在收到响应后解析为结构化数据再发布到 MQTT。

## Features | 功能特性

- Async MQTT bridge based on `aiomqtt`
  基于 `aiomqtt` 的异步 MQTT 桥接
- Modbus request building from raw hex or structured fields
  支持原始十六进制或结构化字段组包 Modbus 请求
- Optional Modbus CRC append and CRC verification
  支持可选 CRC 附加与 CRC 校验
- Flexible parser with typed unpacking, transform script, scale/offset, decimal control, and range filters
  支持类型解包、transform 脚本、scale/offset 转换、小数位控制、区间过滤与忽略区间过滤
- Single-process mode (`dtu.py`)
  单进程模式（`dtu.py`）
- Multi-worker mode (`dtu_multi.py`) with Redis-backed request state
  多 Worker 模式（`dtu_multi.py`），使用 Redis 保存请求状态

## Project Structure | 项目结构

- `dtu.py`: core bridge runtime
  `dtu.py`：核心桥接运行入口
- `dtu_multi.py`: distributed worker runtime
  `dtu_multi.py`：分布式 Worker 入口
- `modbus_data_handler.py`: pack/unpack helpers
  `modbus_data_handler.py`：Modbus 数据打包/解包工具
- `crc.py`: Modbus CRC helpers
  `crc.py`：Modbus CRC 工具
- `config.sample.py`: config template
  `config.sample.py`：配置模板

## Quick Start | 快速开始

### 1) Configure | 配置

```bash
cp config.sample.py config.py
```

Edit `config.py` | 修改 `config.py`：

```python
host = 'localhost'
port = 1883
username = ''
password = ''

# used by dtu_multi.py
periodic_port = 'tcp://localhost:5000'
redis_host = 'redis://localhost:6379'
```

### 2) Install Dependencies | 安装依赖

```bash
pip install -r requirements.txt
```

### 3) Run | 运行

Single process | 单进程:

```bash
python dtu.py
```

Multi-worker mode | 多 Worker 模式:

```bash
python dtu_multi.py
```

## Docker

```bash
docker build -t modbridge-edge:1.0.0 .
docker run -it modbridge-edge:1.0.0
```

## MQTT Topics | MQTT 主题

Bridge subscriptions | 订阅主题：

- `/<tenant>/<device>/request/#`
- `/<tenant>/<device>/dtu/#`

Typical flow | 典型消息流：

1. Cloud sends request to `/<tenant>/<device>/request/<req_id>`
   云端发送请求到 `/<tenant>/<device>/request/<req_id>`
2. Bridge forwards Modbus bytes to `/<tenant>/<device>/dtu/sub`
   网关转发 Modbus 字节流到 `/<tenant>/<device>/dtu/sub`
3. DTU replies on `/<tenant>/<device>/dtu/pub`
   DTU 在 `/<tenant>/<device>/dtu/pub` 返回响应
4. Bridge publishes parsed result to `/<tenant>/<device>/response/<req_id>`
   网关将解析结果发布到 `/<tenant>/<device>/response/<req_id>`

Other outputs | 其他输出主题：

- `/<tenant>/<device>/ping`
- `/<tenant>/<device>/telemetry`
- `/<tenant>/<device>/attributes` (retained)
  `/<tenant>/<device>/attributes`（保留消息）

## Request Format (`method = modbus_req`) | 请求格式（`method = modbus_req`）

### A) Raw Modbus hex | 原始 Modbus 十六进制

```json
{
  "method": "modbus_req",
  "modbus": "0106000f0001",
  "crc": true
}
```

### B) Structured fields | 结构化字段

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": "0001",
  "crc": true
}
```

### C) Typed packing | 按类型打包

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
}
```

### D) Parsing example | 响应解析示例

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "03",
  "reg": "0000",
  "data": "0002",
  "crc": true,
  "parsers": [
    {
      "name": "temperature",
      "unpack_func": "uint16_AB",
      "scale": 0.1,
      "decimal_places": 1,
      "offset": 0,
      "filters": [
        { "type": "range", "min": -40, "max": 85 },
        { "type": "range_ignore", "min": -0.5, "max": 0.5 }
      ]
    }
  ]
}
```

Transform and hex parsing example | transform 与 hex 解析示例：

```json
{
  "method": "modbus_req",
  "addr": "01",
  "op": "03",
  "reg": "0000",
  "data": "0004",
  "crc": true,
  "parsers": [
    {
      "name": "encoded_decimal",
      "unpack_func": "uint16_AB",
      "transform": "digits = str(int(raw_value))\ndecimal_places = int(digits[0])\nmantissa = int(digits[1:])\nmantissa / (10 ** decimal_places)",
      "scale": 1,
      "offset": 0
    },
    {
      "name": "status_hex",
      "unpack_func": "hex16"
    }
  ]
}
```

Response example | 响应示例：

```json
{
  "modbus": "01030400fa0000xxxx",
  "verified": true,
  "temperature": 25.0
}
```

## Concurrency Behavior | 并发行为

- One active request is tracked per device identity.
  每个设备标识同一时间只跟踪一个活跃请求。
- If a previous request is still within timeout window, bridge returns:
  如果前一个请求仍在超时时间内，网关返回：

```json
{
  "modbus_state": "waiting",
  "err": "MODBUS_STATE_WAITING"
}
```

## Notes | 注意事项

- Keep topic identity aligned as `/<tenant>/<device>/...`.
  保持主题中的设备标识一致：`/<tenant>/<device>/...`。
- `unpack_func` must match supported handlers in `modbus_data_handler.py`.
  `unpack_func` 必须与 `modbus_data_handler.py` 中支持的解包函数对应。
- Supported parser pipeline order: `transform -> scale/offset -> decimal_places -> filters`.
  parser 执行顺序为：`transform -> scale/offset -> decimal_places -> filters`。
- `transform` is a full script and receives only `raw_value` as input.
  `transform` 为完整脚本，仅注入 `raw_value` 变量。
- `decimal_places` supports alias `decimal_point`.
  `decimal_places` 支持别名 `decimal_point`。
- Added unpackers: `hex8`, `hex16`, `hex32` (returned as lowercase hex string, not numeric-converted).
  新增解包器：`hex8`、`hex16`、`hex32`（返回小写十六进制字符串，不参与数值转换）。
