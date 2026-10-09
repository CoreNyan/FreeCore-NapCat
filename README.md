# FreeCore-NapCat

FreeCore 社区专用 NapCat QQ 机器人与 CoreNyan 认知中枢双向桥接网关。

---

## 核心架构与职责

`FreeCore-NapCat` 负责将 QQ 官方/NT 协议端（NapCat OneBot 11）与 CoreNyan（Redis 神经中枢总线）无缝衔接：

1. **消息双向路由**：
   - **入站**：接收 QQ 群聊/私聊事件，解析 CQ 码、艾特、命令前缀，清洗后发布至 Redis `freecore:agent:qq:inbound`；
   - **出站**：订阅 Redis `freecore:agent:qq:outbound`，将小可的答复、提示与主动关怀投递至指定 QQ 群或私聊。
2. **动漫手绘铅笔风格卡片渲染（Anime Pencil Card Engine）**：
   - 依赖 `sharp` 图形库，提供高性能手绘漫画风格的服务器状态监控卡片（`/status`）与全节点线路测速卡片（`/line`）。
3. **双向心跳活性探测（Keepalive Heartbeat）**：
   - 15 秒定时双向 Ping-Pong 探测，超期假死连接自动销毁并支持新会话热替换，杜绝孤儿连接。
4. **远程 MC 运维指令调用**：
   - 响应群内服主及管理员的 `/spark` 采样、`/seen` 查询、`/bal` 经济查账等指令。

---

## 核心文件结构

```
FreeCore-NapCat/
├── corenyan-bridge.cjs     # 桥接网关主程序 (OneBot 11 WebSocket 服务端 + Redis 订阅发布 + 图像渲染)
├── corenyan-config.json    # 网关配置文件 (Redis、群权限、卡片主题、服务器与线路列表)
├── package.json            # Node.js 项目依赖清单
├── README.md               # 仓库说明文档
└── .gitignore              # 忽略日志、缓存及本地凭据
```

---

## 快速启动

1. **安装依赖**：
   ```bash
   npm install
   ```
2. **启动网关**：
   ```bash
   node corenyan-bridge.cjs
   ```
3. **NapCat 配置**：
   在 NapCat 的 OneBot 11 配置中启用反向 WebSocket 客户端，连接至 `ws://127.0.0.1:8089/ws` 即可自动连通。
