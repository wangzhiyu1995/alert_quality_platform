# 2026-03-30 事件数阈值口径拆分（方案1）

## 背景
- 历史实现中“告警事件数”维度在单规则评分与数据大盘聚合展示共用同一阈值口径（25 * 统计天数）。
- 这会导致单规则评分偏宽松，且与治理页/画像/建议等单规则视角预期不一致。

## 本次改动

### 1) 单规则口径（规则评分）
- 维度：`event_count`
- 新默认阈值：`1 * period_days`
- 判定：`规则在筛选周期内事件数 <= 1 * 周期天数` 才得该维度权重分。
- 生效范围：
  - 告警治理页「告警规则质量得分」列表中的事件数得分/背景色
  - 画像弹窗中的事件数得分与达标标签
  - 改进建议中“事件数过多”触发逻辑

### 2) 多规则口径（数据大盘聚合）
- 维度：数据大盘「质量维度指标 - 告警事件数」卡片红绿判定
- 新口径：`聚合事件总数 <= K * 周期天数 * 活跃规则数`
- 活跃规则数定义：筛选条件 + 时间窗口内 `distinct rule_id`
- 默认多规则阈值(每天)：`25`

### 3) 配置能力
- 单规则阈值：在「配置管理 -> 权重配置」中配置 `event_count.threshold_value`（默认 `1*period_days`）。
- 多规则阈值：在「配置管理 -> 数据源配置」新增配置项：
  - `event_count_aggregate_threshold_per_day`（默认 `25`）

### 4) 兼容迁移
- 启动时会检查 `event_count` 阈值。
- 若仍是历史默认值（`25*period_days` 等等价写法），自动迁移为新默认 `1*period_days`。
- 自定义阈值不做覆盖。

### 5) 二次迁移（权重页拆分后）
- 为兼容历史“单阈值混用”配置，新增迁移策略：
  - 若检测到 `event_count` 为 `N*period_days` 且 `N>1`，判定其更可能是历史聚合口径。
  - 自动将 `N` 迁移到 `event_count_aggregate_threshold_per_day`（多规则阈值系数）。
  - 同时将单规则阈值重置为 `1*period_days`。
- 目的：避免治理页单规则评分继续使用历史聚合阈值，导致达标口径偏宽。

### 6) 权重配置页面增强
- 在“告警事件数”行新增双阈值输入：
  - `单规则`：沿用 `event_count.threshold_value`（例如 `1*period_days`）
  - `多规则`：保存到 `event_count_aggregate_threshold_per_day`
- UI仅展示单规则与多规则阈值系数，公式换算逻辑不在表格中展开。

## 影响文件
- `/Users/Williwang/dev_project/flask_pj_duty_05_codex/app.py`
- `/Users/Williwang/dev_project/flask_pj_duty_05_codex/models.py`
- `/Users/Williwang/dev_project/flask_pj_duty_05_codex/templates/index.html`

## 回归检查建议
1. 数据大盘：修改时间范围后，事件数卡片红/绿随 `event_count_aggregate_threshold_per_day` 变化。
2. 告警治理页：规则列表事件数列达标与否按单规则阈值生效。
3. 画像弹窗：事件数达标标签与规则列表一致。
4. 改进事项：事件数建议触发与规则得分口径一致。
5. 配置页：
   - 权重配置中 `event_count` 阈值可编辑并生效。
   - 数据源配置中 `event_count_aggregate_threshold_per_day` 可编辑并生效。
