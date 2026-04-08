# sync_data - MongoDB 到 StarRocks 数据同步项目

## 项目简介

这是一个将 MongoDB 的 `nodeDayOpsWide` 集合数据同步到 StarRocks 的 `node_day_ops_wide` 表的工具，支持全量同步和增量同步，并支持断点续传。

## 文件结构

```
sync_data/
├── mongodb2starRocks.py  # 主同步程序
├── sync_incremental.sh   # 增量同步定时执行脚本
├── requirements.txt      # Python 依赖
├── bi_export_import.py   # BI 导出导入工具
├── export_csv.py         # CSV 导出工具
├── fast_sync.py          # 快速同步工具
├── multi_sync.py         # 多线程同步工具
├── sync_shard_*.py       # 分表同步工具
├── sync_simple.py        # 简单同步工具
├── test_query.py         # 查询测试工具
├── sync_checkpoint.json  # 断点文件
├── *.log                 # 各种日志文件
├── logs/                 # 日志目录
├── mongo_data.csv        # 导出的 CSV 数据
├── mongodb-database-tools-ubuntu2204-x86_64-100.10.0/ # MongoDB 工具
└── mongoexport           # MongoDB 导出工具
```

## 配置说明

### MongoDB 配置 (`mongodb2starRocks.py`)

```python
MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
    "batch_size": 10000,
    "incremental_field": "updatedTime",
}
```

### StarRocks 配置

```python
STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
    "table": "node_day_ops_wide",
}
```

## 使用方法

### 全量同步

```bash
cd sync_data
python mongodb2starRocks.py full
```

特点：
- 按 `_id` 排序顺序同步
- 每 `batch_size` 条提交一次并保存断点
- 支持从上次中断处继续同步
- 完成后自动切换到增量模式

### 增量同步

```bash
cd sync_data
python mongodb2starRocks.py incremental

# 或使用脚本（自动记录日志）
./sync_incremental.sh
```

特点：
- 基于 `updatedTime` 字段只同步更新数据
- 使用 `ON DUPLICATE KEY UPDATE` 更新已存在数据
- 自动更新断点，保存最新的 `updatedTime`

## 依赖安装

```bash
pip install -r requirements.txt
```

依赖包：
- pymongo>=4.0.0
- mysql-connector-python>=8.0.0
- pandas>=1.5.0
- sqlalchemy>=2.0.0
- numpy>=1.21.0

## 核心功能

### 类型转换

- `ObjectId` -> 字符串
- `Int64/Int32` -> int
- `Decimal128` -> decimal
- `datetime` -> 格式化字符串 `%Y-%m-%d %H:%M:%S.%f`
- 嵌套文档自动展开扁平化，父键+下划线+子键
- 数组/嵌套对象转换为 JSON 字符串

### 断点续传

- 全量同步：保存最后一个 `_id`
- 增量同步：保存最后一个 `updatedTime`
- 断点文件：`./sync_checkpoint.json`

### 事务处理

- 每批次数据执行成功才提交事务
- 发生异常回滚，保证数据一致性

## 同步字段列表

共 68 个字段，主要包括：

- 客户信息：`customerId`, `customerName`, `purchaserName`, `signatoryName`
- 节点信息：`nodeId`, `name`, `nodeType`, `nodeTags`, `isp`, `province`, `city`
- 带宽指标：`analyzePeak95`, `peak95`, `peak95Ratio`, `eveningPeak95`, `eveningAvg` 等
- 成本信息：`cost_price`, `cost_finalAmount`, `cost_settlement` 等
- 收入信息：`revenue_amount`, `revenue_finalAmount` 等
- 利润信息：`profit_profitAmount`, `profit_profitRate` 等
- 其他：更新时间 `updatedTime`, 快照时间 `snapshotTime` 等
