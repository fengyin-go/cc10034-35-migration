# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## DECIMAL 小数列精度迁移

旧版 Schema同步 把 MySQL 的 DECIMAL 一律固定成 Hive 的 `DECIMAL(38,18)`。
现在源库 introspection 能拿到完整精度，迁移把目标列改成和源一致的
`DECIMAL(p,s)`，并在迁移前逐值检查整数位与小数位——超限就阻塞，绝不
截断或舍入存量金额。

- `backend/app/decimal_types.py`：`DECIMAL(p,s)` 解析与位宽校验（先整数位、后小数位）。
- `backend/app/services/column_ledger.py`：列变更清单，保存数据库原始定义，
  执行结果回写后供下一次部署查询原定义。
- `backend/app/services/ddl_tracker.py`：`capture_from_ddl` 原样捕获 `DECIMAL(p,s)`，不再固定精度。
- `backend/app/services/schema_sync.py`：`convert_type` 保留源精度；生成 Hive 语句与新版小数列迁移记录。
- `backend/app/services/decimal_migration.py`：试运行 / 执行 / 续做 / 下游查询 / 目标核对。

演示批次 `dwd.dwd_order_detail`：`amount DECIMAL(19,4)` 有 3 条历史值装不下
（2 条整数位超限、1 条小数位超限），`fee DECIMAL(10,2)` 可无损迁移，且读 `fee`
的下游不读 `amount`。因此逐列切换时本次发布能提供 `fee`，整批切换时一列都
不能提供——这正是存量保真与整批切换的冲突。

接口（固定路径，先于 `/api/{module}` 注册）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/decimal/dry-run` | 试运行整批，`strict_batch` 切换整批/逐列模式 |
| POST | `/api/decimal/execute` | 执行可迁移列，`columns` 可限定列 |
| POST | `/api/decimal/resume` | 按原版本复查后只重试未生效的列和语句 |
| GET | `/api/decimal/downstream?name=` | 模拟下游按列查询 |
| GET | `/api/decimal/target` / `/verify` | 目标表列定义、存量值、类型冲突核对 |
| GET | `/api/decimal/column-changes` | 列变更清单（含原始定义与执行结果） |
| GET | `/api/decimal/records` / `/runs` | 新版迁移记录与执行历史 |

前端在 Schema同步 页面底部提供同名面板。

测试（标准库 unittest，无需额外依赖）：

```bash
cd backend && .venv/bin/python -m unittest discover -s tests -t . -v
```

领域：数据工程
子方向：数仓与数据湖
