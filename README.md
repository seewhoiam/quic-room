# quic-room

可断线续传的房间协议 + **QUIC 控制流**（Go scaffold）。

- 内存房间：单调 `seq`、快照 + 增量、`resume(fromSeq)`
- 传输：`quic-go`，第一条双向流 = 控制通道
- 编解码：长度前缀 JSON 帧（方便调试）
- 多文件流传输：TODO（后续可在同连接上 `OpenStream`）

## 快速跑

```bash
# 终端 1
go run ./cmd/server -addr 127.0.0.1:4242

# 终端 2
go run ./cmd/client -addr 127.0.0.1:4242 -room demo -name alice

# 终端 3
go run ./cmd/client -addr 127.0.0.1:4242 -room demo -name bob
```

客户端本地演示用 `-insecure`（服务端每次启动自签证书）。

### 断线续传演示

1. alice 连上后随便聊几句  
2. Ctrl+C 退出（会话 token / lastSeq 会写到 `/tmp/quic-room-<room>-<name>.session`）  
3. 再执行同一条 client 命令：会自动 `resume`，补上离线期间的事件  

## 协议摘要

Client → Server: `join` / `chat` / `ack` / `resume` / `ping`  
Server → Client: `welcome` / `event` / `snapshot` / `pong` / `error` / `bye`

事件类型：`member_join` / `member_leave` / `chat`  
事件日志窗口默认 1024；`fromSeq` 过旧则先发 `snapshot` 再追增量。

## 测试

```bash
go test ./...
go build ./...
```

## 代码风格

提交前请跑一下 `gofmt`（仓库根目录的 [.editorconfig](.editorconfig) 已声明 tab 缩进，主流编辑器会自动遵循）：

```bash
gofmt -w .
```
