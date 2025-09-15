# MQTT+UDP 到 WebSocket 桥接服务

## 项目概述

本项目是基于虾哥开源的 [MQTT+UDP 到 WebSocket 桥接服务](https://github.com/78/xiaozhi-mqtt-gateway)，进行了修改，以适应[xiaozhi-esp32-server](https://github.com/xinnan-tech/xiaozhi-esp32-server)。

## 部署使用
部署使用请[参考这里](https://github.com/xinnan-tech/xiaozhi-esp32-server/blob/main/docs/mqtt-gateway-integration.md)。

## 新增功能

1、 新增设备指令下发API，支持MCP指令并返回设备响应。
``` shell
curl --location --request POST 'http://localhost:8007/api/commands/lichuang-dev@@@a0_85_e3_f4_49_34@@@aeebef32-f0ef-4bce-9d8a-894d91bc6932' \
--header 'Content-Type: application/json' \
--data-raw '{"type": "mcp", "payload": {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "self.get_device_status", "arguments": {}}}}'
```

2、 新增设备状态查询API，支持查询设备是否在线。

``` shell
curl --location --request POST 'http://localhost:8007/api/devices/status' \
--header 'Content-Type: application/json' \
--data-raw '{
    "deviceIds": [
        "lichuang-dev@@@a0_85_e3_f4_49_34@@@aeebef32-f0ef-4bce-9d8a-894d91bc6932"
    ]
}'
```