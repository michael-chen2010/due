# M6 启动状态发布竞争回归（2026-10-12）

真实隔离 Auth/双 Gate/双 Game race E2E 首轮在 Game 启动时失败：Node.registerServiceInstances 写 instances 与 Boss 状态 reporter 调用 UpdateMetadata 读 instances 竞争。该失败不能记为账号 E2E GREEN。

修复：初始实例与 Metadata 在 Node.rw 内发布；Metadata 使用独立副本；注册网络调用保持锁外，延续现有快照契约，不改 OwnerEpoch、Session fencing 或 D044。

验收：新增 TestRegisterServiceInstancesConcurrentStartupMetadata 在修复前真实 race RED（固定 cluster-node.test.exe）；修复后该测试、既有 Metadata、Router middleware 和 Session 回归 race 连续 5 轮 PASS。测试构造未启动的回环 linker，不监听端口；执行前核验固定 EXE 精确防火墙规则。完整物理链路尚未通过，继续在 Server 隔离夹具验收。

回滚本提交仅撤销启动竞争修复。未操作既有进程、Docker 或玩家数据。
