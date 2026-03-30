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
- 新口径：`聚合事件总数 <= 多规则阈值(每天) * 周期天数`
- 默认多规则阈值(每天)：`25`

### 3) 配置能力
- 单规则阈值：在「配置管理 -> 权重配置」中配置 `event_count.threshold_value`（默认 `1*period_days`）。
- 多规则阈值：在「配置管理 -> 数据源配置」新增配置项：
  - `event_count_aggregate_threshold_per_day`（默认 `25`）

### 4) 兼容迁移
- 启动时会检查 `event_count` 阈值。
- 若仍是历史默认值（`25*period_days` 等等价写法），自动迁移为新默认 `1*period_days`。
- 自定义阈值不做覆盖。

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
