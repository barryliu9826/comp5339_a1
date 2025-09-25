# 澳大利亚经济数据获取与处理系统

## 项目概述

这是一个综合性的数据获取、处理和分析系统，用于自动下载、处理、存储和可视化澳大利亚的经济和能源数据。系统集成了三个主要数据源，将所有数据统一存储到PostgreSQL数据库中，并提供强大的数据清理和探索性数据分析(EDA)功能。

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
├── data_acquisition_processor.py  # 主处理程序 (545行)
├── database_config.py            # 数据库配置和操作 (820行)
├── geocoding.py                  # 地理编码与缓存 (658行)
├── excel_utils.py                # Excel处理工具 (55行)
├── state_standardizer.py         # 州名标准化工具 (227行)
├── time_format_utils.py          # 时间格式处理工具 (154行)
├── data_cleaner.py               # 统一数据清理模块 (1169行)
└── eda_visualization.py          # 探索性数据分析可视化 (204行)

data/
├── nger_data_api_links.csv       # NGER API链接
├── 14100DO0003_2011-24.xlsx     # ABS Excel数据
├── geocoding_cache.json         # 地理编码持久化缓存
└── eda/                          # EDA可视化输出目录
    ├── abs_overview_geographic_level.png
    ├── cer_map_categories.png
    └── nger_map_by_fuel.png
```

### 技术栈

- **Python 3.11+** (conda环境: comp5339)
- **数据处理**: pandas>=2.3.0, openpyxl>=3.1.0, numpy>=2.3.0
- **数据可视化**: matplotlib>=3.9.0, seaborn>=0.13.2
- **网络请求**: requests>=2.32.0
- **网页爬虫**: selenium>=4.35.0 (Chrome WebDriver)
- **数据库**: PostgreSQL (psycopg2-binary>=2.9.0)
- **地理编码**: Nominatim API (通过requests)
- **Excel处理**: openpyxl (合并单元格解析)
- **类型提示**: typing-extensions>=4.0.0

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

### 使用 pip 安装依赖（推荐方案）

使用内置 `venv` 和 `pip` 安装所有依赖：

```bash
# 1) 创建并激活虚拟环境（macOS/Linux）
python3 -m venv .venv
source .venv/bin/activate

# 2) 升级 pip 并安装依赖
python -m pip install --upgrade pip
pip install -r requirements.txt

# 3) 运行程序
python src/data_acquisition_processor.py
```

### 使用 conda 环境（可选方案）

```bash
# 激活conda环境
conda activate comp5339

# 安装依赖
pip install -r requirements.txt

# 运行程序
python src/data_acquisition_processor.py
```

### 开发环境要求与说明

- **Python 3.11+**（建议）
- **Google Chrome**: Selenium 4.20+ 默认使用 Selenium Manager 自动下载驱动，无需手动配置 chromedriver
- **PostgreSQL**: 本地可用，且账户配置与 `src/database_config.py` 中的 `DB_CONFIG` 一致
- **网络环境**: 如果网络环境受限，地理编码（Nominatim）可能失败或变慢；可多次重试或离线运行仅数据库流程

### 运行数据获取

```bash
# 运行完整的数据获取和处理流程
python src/data_acquisition_processor.py
```

### 运行EDA可视化分析

```bash
# 生成探索性数据分析图表
python src/eda_visualization.py
```

这将生成以下可视化图表并保存到 `data/eda/` 目录：
- **CER电站分布图** (`cer_map_categories.png`): 按类别显示已批准、已承诺和可能建设的电站位置
- **NGER设施分布图** (`nger_map_by_fuel.png`): 按主要燃料类型显示NGER设施的地理分布
- **ABS地理级别概览** (`abs_overview_geographic_level.png`): 显示ABS数据中州级和地方政府级数据的分布情况

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
- **数据清理优化**: 统一的数据清理模块，提高数据处理效率

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
- **数据可视化**: 专业的EDA可视化模块，支持多种图表类型

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
- [x] 统一数据清理模块
- [x] 探索性数据分析可视化

### 📊 数据统计
- **NGER表**: 11张 (按年份: 2013-14 到 2023-24)
- **ABS表**: 15张 (按业务类型，包含两个地理级别)
- **CER表**: 3张 (按电站类型: approved, committed, probable)
- **总数据量**: 约60万+条记录
- **地理编码缓存**: 自动缓存，避免重复API调用
- **EDA可视化**: 3种专业图表类型，支持地理分布和统计分析

## 项目文件说明

### 核心处理文件
- **`src/data_acquisition_processor.py`** (545行): 主数据获取和处理程序
  - NGER数据下载和JSON处理
  - ABS Excel文件读取和合并单元格解析
  - CER网站数据爬取和分页处理
  - 统一的PostgreSQL数据存储
  - 多线程并发处理优化

- **`src/database_config.py`** (820行): 数据库配置和操作模块
  - 数据库连接池管理
  - 表创建和数据插入函数
  - 列名清理和去重逻辑
  - 批量插入优化
  - 线程安全的数据库操作

- **`src/geocoding.py`** (658行): 地理编码与缓存模块
  - Nominatim API集成
  - 内存+文件双层缓存
  - 多线程地理编码处理
  - 线程安全的缓存管理

- **`src/excel_utils.py`** (55行): Excel处理工具模块
  - 合并单元格解析
  - 多级表头处理
  - 动态列名生成

- **`src/state_standardizer.py`** (227行): 州名标准化工具模块
  - 澳大利亚州名标准化映射
  - 支持英文全名、缩写、数字代码转换
  - DataFrame批量州名标准化处理
  - 容错处理和数据清理

- **`src/time_format_utils.py`** (154行): 时间格式处理工具模块
  - NGER年份标签拆分 (如 "2023-24" → start_year, stop_year)
  - CER时间格式标准化处理
  - ABS时间数据格式统一
  - 多种时间格式的自动识别和转换

- **`src/data_cleaner.py`** (1169行): 统一数据清理模块
  - 综合数据清理和规范化功能
  - 缺失值处理和数据类型转换
  - 字符串清理和标准化
  - 数据质量验证和修复
  - 支持多种数据源的统一清理流程

- **`src/eda_visualization.py`** (204行): 探索性数据分析可视化模块
  - CER电站地理分布可视化
  - NGER设施按燃料类型分布图
  - ABS数据地理级别统计分析
  - 专业的matplotlib和seaborn图表生成
  - 自动保存高分辨率图表到指定目录

### 数据文件
- **`data/nger_data_api_links.csv`**: NGER数据API链接
- **`data/14100DO0003_2011-24.xlsx`**: ABS经济数据Excel文件
- **`data/geocoding_cache.json`**: 地理编码持久化缓存文件
- **`data/eda/`**: EDA可视化输出目录
  - `abs_overview_geographic_level.png`: ABS地理级别分布图
  - `cer_map_categories.png`: CER电站类别分布图
  - `nger_map_by_fuel.png`: NGER设施燃料类型分布图

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

### 6. 数据清理与质量保证
- `src/data_cleaner.py` 提供统一的数据清理和规范化功能
- 支持多种数据源的清理流程
- 自动处理缺失值、数据类型转换和字符串标准化
- 数据质量验证和修复机制

### 7. 探索性数据分析可视化
- `src/eda_visualization.py` 提供专业的数据可视化功能
- 支持地理分布图、统计图表等多种可视化类型
- 使用matplotlib和seaborn生成高质量图表
- 自动保存高分辨率图片到指定目录

## 开发历程

### 主要里程碑
1. **数据获取**: 实现三个数据源的自动下载
2. **Excel处理**: 解决复杂合并单元格解析问题
3. **数据库集成**: 统一数据存储到PostgreSQL
4. **性能优化**: 批量插入和连接共享优化
5. **代码重构**: 模块化和精简优化
6. **业务优化**: 数字编码和表结构统一
7. **数据清理**: 统一数据清理和质量保证模块
8. **数据可视化**: 探索性数据分析和可视化功能

### 技术挑战解决
- ✅ NGER数据下载循环问题修复
- ✅ Excel合并单元格多级表头解析
- ✅ CER网站动态内容爬取和分页处理
- ✅ PostgreSQL批量插入性能优化
- ✅ 列名重复和特殊字符处理
- ✅ 数据库连接管理和事务处理
- ✅ 统一数据清理和质量保证机制
- ✅ 专业数据可视化和图表生成

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

## 项目状态

### 当前版本特性
- ✅ 完整的八模块架构 (数据获取、数据库、地理编码、Excel处理、州名标准化、时间格式处理、数据清理、EDA可视化)
- ✅ 多线程并发处理优化
- ✅ 地理编码缓存系统
- ✅ Excel合并单元格智能解析
- ✅ 数据库连接池管理
- ✅ 线程安全的操作设计
- ✅ 州名标准化和时间格式统一处理
- ✅ 统一数据清理和质量保证
- ✅ 专业数据可视化和EDA分析

### 最近更新 (2024年9月26日)
- 🆕 新增 `data_cleaner.py` 模块 (1169行): 统一数据清理和质量保证模块
- 🆕 新增 `eda_visualization.py` 模块 (204行): 探索性数据分析可视化模块
- 🆕 新增 `state_standardizer.py` 模块 (227行): 澳大利亚州名标准化工具
- 🆕 新增 `time_format_utils.py` 模块 (154行): 时间格式处理工具
- 📝 扩展为八模块架构，增强数据处理和可视化能力
- 📝 更新了所有模块的文件行数统计
- 📝 完善了模块功能说明和技术文档
- 📝 更新了requirements.txt，包含数据可视化依赖
- 📝 新增EDA可视化输出目录和图表说明

## 联系信息

- **项目**: COMP5339 Assignment 1
- **环境**: conda comp5339 或 Python venv
- **数据库**: PostgreSQL localhost:5432

---

*最后更新: 2024年9月26日*
