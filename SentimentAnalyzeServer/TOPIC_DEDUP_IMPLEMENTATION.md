# Topic 去重与更新机制实现

## 问题描述
原来的Topic保存逻辑每次都会插入新记录，导致相同topic出现多条流水，样本数据中"中国"这个topic有8条记录，但都是重复的。

## 解决方案
实现了一个"7天内去重"的机制：**当新计算的Topic与已有Topic的topic相同且该已有Topic的updated_at在7天内时，执行UPDATE操作而不是插入新记录。**

## 实现细节

### 1. 数据库层 - SqlServerTopicRepository

#### 新增方法：`find_recent_topic_by_name()`
- **功能**：查找相同topic名称、且updated_at在最近N天内的最新记录
- **参数**：
  - `topic_name`: Topic名称
  - `days_lookback`: 向后查看的天数（默认7天）
- **返回值**：Topic对象（如果存在）或None
- **SQL逻辑**：
  ```sql
  SELECT TOP 1 ... FROM Topic 
  WHERE topic = ? AND updated_at >= (now - 7天)
  ORDER BY updated_at DESC, created_at DESC, id DESC
  ```

#### 新增方法：`update_topic_snapshot()`
- **功能**：更新现有Topic快照，保持主键(created_at, id)不变
- **参数**：
  - `topic`: 新的Topic数据
  - `existing_created_at`: 要更新的Topic的created_at（主键）
  - `existing_id`: 要更新的Topic的id（主键）
- **逻辑流程**：
  1. 提取旧数据用于历史记录
  2. 执行UPDATE操作更新主表
  3. 将旧数据插入到topic_metrics_history表中
  4. 返回更新后的Topic对象

### 2. 业务逻辑层 - TopicDomainService

#### 新增方法：`save_or_update_topic_snapshot()`
- **功能**：自动判断是插入还是更新
- **参数**：
  - `topic`: Topic对象
  - `days_lookback`: 向后查看的天数（默认7天）
- **流程**：
  1. 调用`find_recent_topic_by_name()`检查是否存在最近的相同topic
  2. 如果存在 → 调用`update_topic_snapshot()`执行UPDATE
  3. 如果不存在 → 调用`save_topic_snapshot()`执行INSERT

### 3. 应用服务层 - TopicAppService

#### 修改点1：`recommend_and_cache_topics()`
将：
```python
persisted_topic = self.topic_domain_service.save_topic_snapshot(enriched_topic)
```
改为：
```python
persisted_topic = self.topic_domain_service.save_or_update_topic_snapshot(enriched_topic, days_lookback=7)
```

#### 修改点2：`_flush_cache_topics_to_db()`
将：
```python
persisted = self.topic_domain_service.save_topic_snapshot(topic)
```
改为：
```python
persisted = self.topic_domain_service.save_or_update_topic_snapshot(topic, days_lookback=7)
```

### 4. 抽象层 - TopicRepository
在抽象类中添加了两个新方法的声明：
- `find_recent_topic_by_name()` - 查找最近的相同topic
- `update_topic_snapshot()` - 更新现有Topic快照

## 数据流变化

### 之前（有问题）：
```
新Topic A (中国) → INSERT → 新记录 (id=7, created_at=1774532450)
新Topic A (中国) → INSERT → 新记录 (id=24, created_at=1774532480)
新Topic A (中国) → INSERT → 新记录 (id=44, created_at=1774534260)
... （多个重复记录）
```

### 之后（改进后）：
```
新Topic A (中国) → find_recent_topic_by_name() → 找到旧Topic B (中国, updated_at=1774536074)
                → update_topic_snapshot() → UPDATE Topic SET ... WHERE created_at=1774536074 AND id=142
                → INSERT INTO topic_metrics_history (保存旧B数据用于历史分析)
```

## 历史记录处理

更新时，旧的Topic数据会自动保存到`topic_metrics_history`表，用于：
- 热度变化趋势分析（heat_change_percent计算）
- Topic生命周期追踪（Inception → Growth → Climax → Decline）
- 时间序列数据完整性保证

## 影响范围

- ✅ 推荐Topic流程自动应用去重
- ✅ 缓存同步到数据库自动应用去重
- ✅ 历史数据自动保留，不影响趋势分析
- ✅ 主键(created_at, id)保持稳定，关键字不变

## 风险与考虑

1. **回溯期设置**：目前固定7天，可根据业务需求调整
2. **并发更新**：SQL Server的IDENTITY+UPDATE操作是原子的，安全
3. **版本号**：更新时version自动+1，便于追踪
4. **空值处理**：所有optional字段都有null检查
