# local-mqtt-broker-go

一个用 Go 实现的本地 **MQTT 3.1.1** 代理，基于**真实 TCP 连接**模拟多个设备与网关之间的消息收发，
重点保证不同 QoS、会话状态和异常断开下投递结果**一致、可复现、可解释**（每个关键事件都有结构化日志）。

## 支持的协议范围

遵循 OASIS MQTT 3.1.1 报文格式与状态机：

- `CONNECT` / `CONNACK`：协议名校验（仅 `"MQTT"`，级别 4）、保留位校验、空 Client ID 规则、
  Keep Alive、遗嘱（Will）、用户名/密码字段结构解析（本实现不做鉴权）。
- `SUBSCRIBE` / `SUBACK`、`UNSUBSCRIBE` / `UNSUBACK`：多个主题过滤器、逐过滤器返回授权 QoS（0/1/2）
  或失败码 `0x80`。
- `PUBLISH` / `PUBACK` / `PUBREC` / `PUBREL` / `PUBCOMP`：QoS 0/1/2 完整流程。
- `PINGREQ` / `PINGRESP`、`DISCONNECT`。
- 主题过滤器通配符（`+` 单层、`#` 多层，`a/#` 同时匹配 `a` 与 `a/...`），
  非法发布主题与非法过滤器（如 `a/#/c`、`a/b+`）会被拒绝。
- 保留消息（Retained）：订阅建立时回放、空负载清除、按订阅 QoS 降级。
- 遗嘱消息（Will）：异常断开发布、正常 `DISCONNECT` 抑制、支持保留遗嘱。
- 同 Client ID 重连接管旧连接；清洁/持久会话的 `CONNACK sessionPresent` 语义。

明确**不支持**（有意的范围取舍）：MQTT 5、TLS/WebSocket、鉴权与 ACL、共享订阅、
`$SYS` 主题、MQTT 3.1（`MQIsdp`）协议名。

## QoS 与投递语义

- **QoS 0**：至多一次，直接下发，不持久化、无确认。
- **QoS 1**：至少一次。下发前以 Packet ID 为键持久化到会话；
  收到 `PUBACK` 才删除。连接中断后重连，未确认的消息以**同一 Packet ID 且 DUP=1** 重发，
  不会产生新副本；重复 `PUBACK` 幂等处理。
- **QoS 2**：恰好一次。
  - 入站（客户端发布给 Broker）：`PUBLISH -> PUBREC`，Broker 暂存消息且**不**下发给订阅者；
    `PUBREL` 后才执行发布并回 `PUBCOMP`。重复的 PUBLISH 只重复应答、应用消息只下发一次；
    PUBREL 之后又来的 PUBLISH 按协议错误断连。
  - 出站（Broker 发给订阅者）：记录两阶段状态，重连时若已进入第二阶段则只重发 `PUBREL`，
    不重复发送 PUBLISH。
- **QoS 降级**：实际投递 QoS = min(发布 QoS, 订阅授权 QoS)。

## 会话语义（可观察的差异）

- 清洁会话（CleanSession=1）：断开即删除订阅与所有未完成状态；离线期间的 QoS 1/2 消息丢弃。
- 持久会话（CleanSession=0）：订阅、入站 QoS2 半状态、出站在途消息和离线队列均保留；
  重连时 `sessionPresent=1`，先重放未确认在途消息（DUP=1），再投递离线队列。
- 同一 Client ID 新连接会接管并断开旧连接（遗嘱会发布，因为对旧连接属于异常断开）。

## 异常断开与心跳

- 正常断开：客户端发 `DISCONNECT`，抑制遗嘱。
- 异常断开：TCP 关闭、RST、读错误、心跳超时、慢消费者、被接管 —— 均发布遗嘱并记录具体原因。
- 心跳：超时阈值为 `1.5 × Keep Alive`（MQTT 3.1.1 强制）。判定基于 Go 的单调时钟
  （`time.Since(time.Now())`），系统墙钟跳变（NTP 校时等）不会造成误触发；
  任意报文（不仅是 PINGREQ）都会刷新活动时间。

## 资源限制与取舍

`broker.Config` 可配置（括号内为默认值）：

- `MaxPacketSize`（256 KiB）：单报文上限，超限或畸形剩余长度立即断连。
- `MaxTopicLevels`（32）：主题层级深度上限。
- `MaxSubscriptions`（128）：每个会话的订阅数上限，超限过滤器在 SUBACK 中返回 `0x80`。
- `MaxSessionQueue`（256）：持久会话离线 QoS 1/2 队列长度，满时丢弃最旧消息并记录日志。
- `WriteQueueDepth`（64）：每连接独立的写通道深度。慢消费者先背压 2 秒，仍无法消费则断开该连接；
  其他连接有独立 goroutine 与队列，**不会被阻塞**（有专门的多设备扇出/慢消费者测试）。
- Packet ID 按会话独立分配（1..65535）；全部占满时消息转入会话队列，待 ACK 释放 ID 后再投递。

单连接的协议错误只关闭该连接，代理与其他连接继续工作（测试中明确验证）。

## 目录结构

- `internal/mqtt3`：报文编解码、剩余长度、UTF-8 字符串校验、主题与过滤器校验/匹配。
- `internal/broker`：TCP 接入、连接状态机、会话、订阅 Trie、保留消息、QoS 状态机、遗嘱与心跳。
- `internal/testclient`：测试专用的最小 MQTT 3.1.1 真实 TCP 客户端（支持畸形报文注入）。
- `cmd/mqtt-broker`：可执行入口。

## 运行

```bash
go run ./cmd/mqtt-broker -addr 127.0.0.1:1883
```

## 验证方法

```bash
# 全量测试（日志会同时打印 broker 事件与“业务/用例/结果”覆盖行）
go test -v -count=1 ./...

# 并发数据竞争检测（需要 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...
```

测试日志中有两类可辨识输出：

- `[broker] event=...`：代理内部事件（connect/publish/route/redeliver/will/dupack/
  subscribe_reject/queue_overflow/disconnect reason=... 等），可直接解释每条消息的投递轨迹。
- `业务=... 用例=... 结果=PASS/FAIL ...`：测试自报的业务覆盖矩阵。

### 覆盖的业务场景

| 业务 | 代表用例 |
| --- | --- |
| 通配符订阅 | 精确/`+`/`#`/空层级匹配；TCP 端到端 `floor1/#` 跨层投递 |
| 主题合法性 | 非法过滤器拒绝、PUBLISH 主题禁止通配符 |
| 连接校验 | 错误协议名 0x01、空 ClientID 0x02、保留位非法、CONNECT 前报文、同 ID 接管 |
| 畸形报文 | 错误固定头标志、截断报文断连，且不影响其他连接 |
| QoS0 | 扇出与无确认投递 |
| QoS1 | 离线缓存、同 ID + DUP 重发、不重复、清洁会话丢弃、重复 PUBACK 幂等 |
| QoS2 | 四次握手、重复 PUBLISH 去重、PUBREL 后才下发 |
| QoS 降级 | QoS2 发布到 QoS0 订阅按 QoS0 投递 |
| 会话状态 | 持久会话恢复订阅、清洁会话不保留、sessionPresent |
| 保留消息 | 订阅回放（Retain 标志）、空负载清除 |
| 遗嘱消息 | 异常断开发布、正常断开抑制 |
| 心跳超时 | 静默超 1.5 倍关闭、抖动 PING 不误杀（单调时钟） |
| 订阅限制 | 数量超限与非法过滤器返回 0x80 |
| 慢消费者 | 慢连接被隔离/断开，快连接持续收消息 |
| 多设备并发 | 8 设备 × 6 条 QoS1 汇聚到网关，48/48 不丢不重 |
