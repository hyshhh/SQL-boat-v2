# Demo 目标框颜色机制分析

## 颜色决策逻辑

文件：[pipeline/demo.py](../pipeline/demo.py#L48-L61)

```python
# 优先级从高到低，命中即停
if track_info and track_info.db_matched:              # → 绿色 (0, 200, 0)
    color = (0, 200, 0)
elif track_info and track_info.recognized and \
     track_info.hull_number and track_info.semantic_match_ids:  # → 黄色 (0, 215, 255)
    color = (0, 215, 255)
elif track_info and track_info.recognized and track_info.hull_number:  # → 红色 (0, 0, 255)
    color = (0, 0, 255)
elif track_info and track_info.recognized and \
     not track_info.hull_number and track_info.semantic_match_ids:  # → 红色
    color = (0, 0, 255)
elif track_info and track_info.recognized and not track_info.hull_number:  # → 红色
    color = (0, 0, 255)
elif track_info and track_info.pending:                # → 青色 (255, 255, 0)
    color = (255, 255, 0)
else:                                                  # → 灰色 (180, 180, 180)
    color = (180, 180, 180)
```

## Track 状态字段

文件：[pipeline/tracker.py](../pipeline/tracker.py#L17-L31)

| 字段 | 类型 | 说明 |
|------|------|------|
| `db_matched` | `bool` | 是否精确匹配过，**只升不降** |
| `recognized` | `bool` | 是否已完成 VLM 识别 |
| `hull_number` | `str` | 识别到的弦号（可被覆盖） |
| `semantic_match_ids` | `list[str]` | 语义候选弦号列表 |
| `pending` | `bool` | 是否正在等待 VLM 结果 |

## Bug：精确匹配后框色锁死为绿色

### 现象

一旦某个 Track 发生过精确匹配（如 Track 4 匹配到弦号 "320"），此后**所有帧**中该 Track 的目标框都显示绿色，即使后续识别推翻了之前的结论。

实际日志示例：

```
15:39:28  [Track 4] 弦号：320，精确匹配     → 框色：绿色 ✓
15:39:39  [Track 4] 弦号：(无)，相似：['320', '174']  → 框色：仍是绿色 ✗（应该是黄色或红色）
```

### 根因

**`db_matched` 是"终身成就奖"——只升不降，永不回退。**

1. `bind_db_match()`（[tracker.py:89-95](../pipeline/tracker.py#L89-L95)）只设 `True`，没有对应的清除逻辑。
2. `bind_result()`（[tracker.py:78-87](../pipeline/tracker.py#L78-L87)）虽然会**覆盖** `hull_number` 和 `description`，但**不会触碰** `db_matched`。
3. `_render_detection()` 中 `db_matched` 优先级最高，一旦为 `True` 直接返回绿色，不检查其他字段。

### 时序推演

| 时间 | 事件 | hull_number | db_matched | semantic_match_ids | 框色 |
|------|------|-------------|------------|---------------------|------|
| T1 | Track 4 首次识别，精确命中 "320" | "320" | True | — | 绿色 |
| T2 | Track 4 再次识别，无弦号，仅有语义候选 | ""（被覆盖） | True（未动） | ['320','174'] | **仍绿色** |

T2 时刻：
- `bind_result` 覆盖 `hull_number = ""`，更新 `description`
- `_handle_agent_result` 中 `match_type != "exact"` → 不调用 `bind_db_match` → `db_matched` 保持 `True`
- `match_type == "semantic"` → 调用 `bind_semantic_matches`，更新了 `semantic_match_ids`
- 但渲染时 `db_matched` 在最前面 → 直接绿色，后续字段全部被忽略

### 连带问题：Track 为何被多次识别？

`needs_recognition()`（[tracker.py:50-55](../pipeline/tracker.py#L50-L55)）在 `recognized = True` 后返回 `False`，理论上不会触发第二次识别。但实际日志显示同一 Track 被识别了两次（间隔 11 秒），可能原因：

- 多帧并发提交：第一帧的 VLM 结果还没返回时，后续帧已把同一 track 再次送入识别队列
- 此时 `pending` 刚被设为 `True` 但 `recognized` 还是 `False`，`needs_recognition` 返回 `False`（因为 `pending = True`）
- 但如果 `pending` 设置和检查之间存在竞态，可能导致重复提交

## 已实施的优化：精确匹配后跳过定时刷新

**结论**：`db_matched=True` 后框色锁死为绿色是正确的设计——一旦精确匹配就不该再变。但需要避免对已精确匹配的 track 做无意义的重复 VLM 调用。

### 改动（已实施）

新增 config 开关 `pipeline.skip_refresh_matched`：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `pipeline.skip_refresh_matched` | `false` | `true` 时，`db_matched=True` 的 track 跳过定时刷新 |

涉及文件：
- [config.yaml](../config.yaml) / [config.py](../config.py) — 新增配置项
- [pipeline/tracker.py](../pipeline/tracker.py#L57-L66) — `needs_refresh()` 新增 `skip_matched` 参数
- [pipeline/pipeline.py](../pipeline/pipeline.py) — 读取配置并传入 tracker

### 行为对比

| 模式 | `skip_refresh_matched: false`（旧） | `skip_refresh_matched: true`（新） |
|------|--------------------------------------|-------------------------------------|
| 精确匹配的 track | 每隔 `gap_num` 帧重新识别 | **不再识别**，框色保持绿色 |
| 未精确匹配的 track | 每隔 `gap_num` 帧重新识别 | 每隔 `gap_num` 帧重新识别 |
| 节省的 VLM 调用 | 无 | 精确匹配的 track 不再消耗推理资源 |

## 遗留问题

3. **防止重复识别**：在提交 VLM 任务前加锁或原子检查，确保同一 Track 不会并发提交多次。
3. **防止重复识别**：在提交 VLM 任务前加锁或原子检查，确保同一 Track 不会并发提交多次。
