# 松原 · 分析算子执行剖面仪

松原数据库教学研发组 6 人用真实 Python scan/filter/hash join/group 算子，复查一条分析查询的每一步输出。

## 场景与模块
上游是设备可靠性课程的采样文件、传感器表和分析需求。过去在白板画算子、手动算中间行数，再把截图发给助教；重复键丢失、空结果误判、输入版本变化后无法复现实验是三个痛点。本站保存数据集与有界计划 DSL，worker 真执行并保存不可变快照/逐步摘要，下游讲师和同学只读复查。

数据集录入/编辑、算子计划编排、执行队列、算子剖面/JSON导出四个业务页面；管理员有审计。三个实体 datasets / plans / traces 均贯穿数据库、模型、schema、service、controller、route、前端api/store/page。

## 启动
```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
前端 http://localhost:18244 ；后端 http://localhost:19244/docs ；PostgreSQL localhost:17744，trace / tracepass / operatortrace。结束 `docker compose down -v --remove-orphans` 会删本地数据。

| 角色 | 账号 | 页面与权限 | 动作 |
|---|---|---|---|
| admin | admin@operator.local | 全部含审计 | 复核执行人与完成事件 |
| operator | operator@operator.local | 创建/编辑/执行/导出 | 新建计划并交给worker |
| viewer | viewer@operator.local | 只读和导出 | 观察轨迹，写接口403 |

密码统一 `Trace2026!`。密码PBKDF2，JWT八小时；演示凭据和密钥仅限本地，部署前更换。

## 一天剧本与种子
1. 09:00 operator 登录读取「松原日班温升采样」。
2. 09:05 查看12传感器、240采样，北场/南场/试验间三分区。
3. 09:10 新建 `shift-20261117` 数据集，备注 `daily-readings-20261117.json`。
4. 09:15 保存高于50度→按sensor_id连接→按zone平均的计划。
5. 09:20 提交 `shift-20261117`，观察worker排队到完成。
6. 09:25 逐算子复查输入/输出行数与结果预览。
7. 09:30 导出 `trace-<id>.json`，viewer只读验证。
8. 10:00 admin 查看审计。

seed三账号、三个数据集（240行日班、空数据、重复右键）、三个对应计划。固定公式，无随机数；自然键补缺不覆盖用户修改，连续运行不新增。

## 算法与边界
这是教学内核模型，不是PostgreSQL执行器剖面。PostgreSQL真实保存业务元数据；Python worker真实计算表扫描、数值/相等筛选、构建哈希桶与内连接、Decimal累积分组。右表重复键保留倍数，空集合输出空集合。分组avg/sum保留6位小数，输出按规范JSON排序，SHA-256使用排序key紧凑JSON UTF-8；相同输入和计划结果摘要相同。每步输出输入/输出行数、耗时、明细、前5行与checksum。scan 的 input_rows=0 表示从源读取，detail.scanned_rows为扫描源行数；row_visits是扫描/过滤/哈希构建与探测/聚合各步逻辑行访问总和，不代表物理I/O。

DSL最多6步，首步仅scan readings；filter eq/gte/lte；hash_join仅readings.sensor_id=sensors.sensor_id且最多一次；group仅value的count/sum/avg。禁止eval、任意代码和任意SQL。最多100传感器、2000采样、10000连接输出行；超限失败，不保存部分结果。JSON非法、字段错误、非法算子、孤立引用、重复名有可恢复错误。

## 按资源份额调度
执行器不再一次只跑一个剖面。每次执行在提交时按数据集规模**申报整数份额（1–8，固定公式：每250采样或每25传感器各1份并夹到区间内，不可变）**，管理员设置**全局容量**与**各数据集容量**两类份额上界。领取需同时满足两类容量，足够即可并发，不足则 FIFO 排队。

- **空闲份额先跑小任务，但有明确放行上界**：队首任务因容量不足阻塞时，后面能放下的小任务可占用空闲份额先行；但每个排队任务被更晚任务**插队（越过）的次数有硬上界 `MAX_SKIP=2`**。计数达到上界的任务成为硬屏障，本次调度不再放行任何更晚任务。因此可容纳的大任务至多被两个小任务越过，之后严格按序等待，不会无限等待（无饿死）。
- **在线缩容**：管理员随时调小容量，在途执行保留不打断；超占期间（当前占用 > 新容量）暂停新领取，待占用自然回落到新容量以内再恢复。
- **取消语义**：取消排队任务由 Lua 脚本原子出队并立即落 `cancelled` 终态；取消运行任务只置取消标记，worker 在**算子步边界**停止计算后才归还份额。取消的执行不发布结果（导出返回 409）。
- **原子与幂等**：领取、取消、释放均为单个 Redis Lua 脚本。并发领取不重复占用；重复取消不重复归还；成功、失败、取消都恰好释放一次（token 校验 + 幂等返回 `done/cancelled/absent`）。
- **页面呈现**：执行队列页展示每个任务的等待原因（全局/数据集份额不足、放行上界屏障、超占暂停、超出容量、等待轮询）、已被插队次数、申报份额与占用 worker，以及全局/各数据集容量占用和终态；管理员可在线调整容量。

任务状态在 queued/running/completed/failed 之外新增 cancelled。进程崩溃后的 running 任务仍不自动重试，保留供人工复查。

## 技术、目录与本地开发
Python3.11 / FastAPI / SQLAlchemy / psycopg / Redis / PostgreSQL16；原生 HTML/CSS/JS ES modules / nginx1.27。五服务frontend/backend/worker/db/redis均健康探针，命名卷数据库/Redis/共享产物。DB/Redis健康后启动backend，backend健康后启worker/frontend。worker 进程内按 `WORKER_CONCURRENCY` 并发领取，多副本即多执行器争抢（Lua 脚本保证不重复占用）。

backend/src按models/schemas/services/controllers/routes/middlewares/config/constants/utils分层；backend/scripts/seed_demo.py；backend/migrations元数据建表版本。frontend/src按api/stores/pages/components/common/hooks/router/types/constants/utils分层。三共享组件table/status/feedback，两hooks forms/poll。角色/状态/算子枚举在backend/src/constants/enums.py与frontend/src/constants/enums.js；权限/auth、worker状态、operator校验、前端状态展示使用相同枚举值。请求追踪和全局错误中间件、JWT授权、业务审计贯穿模块。

石墨黄铜工程蓝图视觉，双字体Noto Sans SC/Space Mono来自Google Fonts，离线回退；响应桌面/平板/手机。环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、JWT_SECRET、SEED_DEMO。DATABASE_URL/REDIS_URL由Compose提供。SEED_DEMO启动失败只记日志。

本地后端：pip install -r backend/requirements.txt，设置DATABASE_URL、REDIS_URL、JWT_SECRET，在backend运行uvicorn src.main:app --reload；前端需代理/api，不支持file://。截图、测试、日志和报告在批次根test-artifacts/gb-4105，源码无测试产物。License MIT。

## 调度测试
后端测试位于 `backend/tests`（依赖见 `requirements-dev.txt`），使用真实 Redis（默认 `redis://127.0.0.1:16399/0`，可用 `REDIS_URL` 覆盖）与 SQLite，无需 PostgreSQL：
```sh
pip install -r backend/requirements-dev.txt
redis-server --port 16399 --save "" --appendonly no   # 任一可达 Redis 即可
cd backend
REDIS_URL=redis://127.0.0.1:16399/0 JWT_SECRET=test-secret \
  DATABASE_URL=sqlite+pysqlite:////tmp/operator_trace_test.db \
  python -m pytest tests/ -q
```
- `test_scheduler.py`：申报份额公式、容量并发放行、空闲份额先跑小任务、`MAX_SKIP=2` 放行上界与硬屏障、并发领取不重复占用、释放/取消幂等、缺键回落默认容量。
- `test_integration.py`：真实 worker 线程下的多执行器争抢、排队/运行取消、取消撞完成不发布结果、失败释放份额。
- `test_http_api.py`：认证与角色权限、管理员容量接口、提交申报与列表调度态、取消幂等与取消后禁止导出。
