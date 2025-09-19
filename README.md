# 澳大利亚经济数据获取与处理系统

## 项目概述

这是一个综合性的数据获取和处理系统，用于自动下载、处理和存储澳大利亚的经济和能源数据。系统集成了三个主要数据源，并将所有数据统一存储到PostgreSQL数据库中。

## 数据源

### 1. NGER数据 (National Greenhouse and Energy Reporting)
- **数据年份**: 2013-14 到 2023-24 (11个年份)
- **数据格式**: JSON API响应
- **存储方式**: 按年份分表存储 (`nger_2013_14`, `nger_2014_15`, ...)
- **数据量**: 每年约400-800条设施记录

### 2. ABS数据 (Australian Bureau of Statistics)
- **数据文件**: `14100DO0003_2011-24.xlsx`
- **数据内容**: 澳大利亚经济和行业数据
- **地理级别**: 
  - **州级数据** (geographic_level = 0): 州、领地和统计区域级别
  - **地方政府级数据** (geographic_level = 1): 地方政府区域级别
- **存储方式**: 按业务类型分表，每表包含两个地理级别的数据
- **数据量**: 州级约29,097行，地方政府级约5,477行

### 3. CER数据 (Clean Energy Regulator)
- **数据来源**: CER官网表格数据
- **数据类型**: 
  - `cer_approved_power_stations`: 已批准电站 (~280条记录)
  - `cer_committed_power_stations`: 已承诺电站 (~35条记录)
  - `cer_probable_power_stations`: 可能建设电站 (~49条记录)
- **获取方式**: Selenium自动化网页爬取

## 系统架构

### 核心文件

```
src/
├── data_acquisition_processor.py  # 主处理程序 (513行)
├── database_config.py            # 数据库配置和操作 (267行)
└── geocoding.py                  # 地理编码与缓存（已集成）

data/
├── nger_data_api_links.csv       # NGER API链接
├── 14100DO0003_2011-24.xlsx     # ABS Excel数据
└── geocoding_cache.json         # 地理编码持久化缓存

backup/                           # 历史版本备份
```

### 技术栈

- **Python 3.11+** (conda环境: comp5339)
- **数据处理**: pandas, openpyxl, numpy
- **网络请求**: requests
- **网页爬虫**: selenium (Chrome WebDriver)
- **数据库**: PostgreSQL (psycopg2)

## 数据库设计

### 连接配置
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres', 
    'password': 'postgre',
    'database': 'postgres'
}
```

### 表结构设计

#### NGER表 (按年份分表)
```sql
CREATE TABLE nger_2023_24 (
    id SERIAL PRIMARY KEY,
    facility_name TEXT,
    state TEXT,
    postcode TEXT,
    -- 其他动态列...
);
```

#### ABS表 (按业务类型分表)
```sql
CREATE TABLE business_entries__year_ended_30_june (
    id SERIAL PRIMARY KEY,
    code TEXT,                    -- 地区代码
    label TEXT,                   -- 地区名称
    year INTEGER,                 -- 年份
    geographic_level INTEGER,     -- 地理级别 (0=州级, 1=地方政府级)
    -- 业务数据列...
);
```

#### CER表 (按电站类型分表)
```sql
CREATE TABLE cer_approved_power_stations (
    id SERIAL PRIMARY KEY,
    accreditation_code TEXT,
    power_station_name TEXT,
    state TEXT,
    postcode TEXT
);
```

### 地理级别编码

| 编码 | 含义 | 数据来源 | 记录数量 |
|------|------|----------|----------|
| 0 | 州级政府 | Table 1 | ~29,097行 |
| 1 | 地方政府级 | Table 2 | ~5,477行 |

## 使用方法

### 环境准备

```bash
# 激活conda环境
conda activate comp5339

# 确保PostgreSQL运行
# 用户名: postgres, 密码: postgre, 端口: 5432
```

### 运行数据获取

```bash
# 运行完整的数据获取和处理流程
python src/data_acquisition_processor.py
```

### 数据查询示例

```sql
-- 查询2023年州级商业数据
SELECT * FROM business_entries__year_ended_30_june 
WHERE geographic_level = 0 AND year = 2023;

-- 统计不同级别的数据量
SELECT 
    CASE geographic_level 
        WHEN 0 THEN '州级'
        WHEN 1 THEN '地方政府级'
    END as level_name,
    COUNT(*) as count
FROM business_entries__year_ended_30_june 
GROUP BY geographic_level;

-- 查询特定年份的NGER设施
SELECT facility_name, state, postcode 
FROM nger_2023_24 
WHERE state = 'NSW';

-- 查询已批准的电站
SELECT power_station_name, state 
FROM cer_approved_power_stations 
WHERE state = 'NSW';
```

## 系统特性

### 🚀 性能优化
- **连接共享**: 所有数据源共享单一数据库连接
- **批量插入**: 10,000条记录为一批，优化大数据量处理
- **数字编码**: 使用INTEGER类型替代TEXT，提升查询性能
- **智能去重**: 自动处理重复列名和空列
 - **地理编码缓存**: 内存+文件双层缓存，避免重复调用，提高速度

### 🛡️ 错误处理
- **连接管理**: 自动重试和连接恢复
- **事务回滚**: 操作失败时自动回滚
- **数据验证**: 自动过滤无效数据和空列
- **异常日志**: 详细的错误信息和处理状态
 - **缓存安全**: 保存缓存时直接覆盖，不再创建.backup

### 🔧 代码质量
- **模块化设计**: 数据库操作与业务逻辑分离
- **函数复用**: 统一的列名清理和数据处理逻辑
- **类型安全**: 严格的类型注解和参数验证
- **文档完善**: 详细的函数文档和使用说明

## 数据获取状态

### ✅ 已完成
- [x] NGER数据下载 (2013-2024年，11个文件)
- [x] ABS经济数据下载和处理
- [x] CER网站数据爬取
- [x] PostgreSQL数据库集成
- [x] 表结构优化和统一
- [x] 批量数据插入优化
- [x] 连接共享和性能优化
- [x] 代码模块化和精简

### 📊 数据统计
- **NGER表**: 11张 (按年份)
- **ABS表**: 15张 (按业务类型，包含两个地理级别)
- **CER表**: 3张 (按电站类型)
- **总数据量**: 约60万+条记录

## 项目文件说明

### 核心处理文件
- **`src/data_acquisition_processor.py`**: 主数据获取和处理程序
  - NGER数据下载和JSON处理
  - ABS Excel文件读取和合并单元格解析
  - CER网站数据爬取和分页处理
  - 统一的PostgreSQL数据存储

- **`src/database_config.py`**: 数据库配置和操作模块
  - 数据库连接管理
  - 表创建和数据插入函数
  - 列名清理和去重逻辑
  - 批量插入优化

### 数据文件
- **`data/nger_data_api_links.csv`**: NGER数据API链接
- **`data/14100DO0003_2011-24.xlsx`**: ABS经济数据Excel文件

### 文档文件
- **`data_dictionary.md`**: 数据字典和编码说明
- **`README_merged_cells.md`**: Excel合并单元格处理说明

## 技术亮点

### 1. Excel合并单元格处理
- 自动识别和解析Excel中的合并单元格
- 支持多级表头结构
- 动态生成列名和表结构

### 2. 网页数据爬取
- Selenium自动化爬取动态网页内容
- 智能分页检测和处理
- 表格类型自动识别

### 3. 数据库优化
- 动态表结构生成
- 批量数据插入优化
- 智能列名清理和去重
- 连接池化管理

### 4. 代码架构
- 模块化设计，职责分离
- 统一的错误处理机制
- 可扩展的数据源集成架构
- 完善的日志和监控

### 5. 地理编码与缓存
- 使用 Nominatim API（遵守速率限制，约1.1秒/请求）
- `src/geocoding.py` 提供地理编码与缓存（内存+JSON持久化）
- 多线程环境下使用线程安全的全局缓存
- 主流程结束时自动保存 `data/geocoding_cache.json`

## 开发历程

### 主要里程碑
1. **数据获取**: 实现三个数据源的自动下载
2. **Excel处理**: 解决复杂合并单元格解析问题
3. **数据库集成**: 统一数据存储到PostgreSQL
4. **性能优化**: 批量插入和连接共享优化
5. **代码重构**: 模块化和精简优化
6. **业务优化**: 数字编码和表结构统一

### 技术挑战解决
- ✅ NGER数据下载循环问题修复
- ✅ Excel合并单元格多级表头解析
- ✅ CER网站动态内容爬取和分页处理
- ✅ PostgreSQL批量插入性能优化
- ✅ 列名重复和特殊字符处理
- ✅ 数据库连接管理和事务处理

## 未来扩展

### 待实现功能
- [ ] 数据质量监控和报告
- [ ] 自动化数据更新调度
- [ ] 数据可视化仪表板
- [ ] API接口开发

### 性能优化空间
- [ ] 数据库索引优化
- [ ] 查询性能调优
- [ ] 并发处理优化

## 地理编码与缓存使用说明

### 自动集成（推荐）

运行主处理程序时会自动对 CER 表进行地理编码并写入缓存：

```bash
python src/data_acquisition_processor.py
```

### 在代码中使用

```python
from geocoding import Geocoder, save_global_cache

geocoder = Geocoder(use_persistent_cache=True)
result = geocoder.geocode_query("Sydney, NSW, Australia")

# 退出前可手动保存（主流程已自动保存）
save_global_cache()
```

### 缓存文件

- 位置: `data/geocoding_cache.json`
- 保存策略: 直接覆盖保存，不再生成 `.backup` 文件

## 联系信息

- **项目**: COMP5339 Assignment 1
- **环境**: conda comp5339
- **数据库**: PostgreSQL localhost:5432

---

*最后更新: 2025年9月18日*
