# PostgreSQL/Hive Schema 提取器 + SQL生成器

这是一个用于提取数据库模式(schema)信息并使用DeepSeek AI生成SQL查询语句的工具。目前支持PostgreSQL和Hive数据库。

## 功能

1. **数据库模式提取**：提取数据库中的表结构、列定义和约束信息
   - 支持PostgreSQL的表、列、约束信息提取
   - 支持Hive的表、列、分区信息提取
2. **SQL查询生成**：基于提取的数据库结构，使用DeepSeek AI生成SQL查询语句
   - 根据数据库类型自动生成适用的SQL语法

## 安装与配置

### 依赖安装

对于PostgreSQL支持：
```bash
pip install psycopg2 python-dotenv requests
```

对于Hive支持：
```bash
pip install pyhive[hive] thrift thrift-sasl sasl
```

或者安装所有依赖：
```bash
pip install -r requirements.txt
```

### 配置环境变量

修改`.env`文件，填入您的数据库连接信息和DeepSeek API密钥：

```
# PostgreSQL连接配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password

# Hive连接配置
HIVE_HOST=localhost
HIVE_PORT=10000
HIVE_DATABASE=default
HIVE_USER=your_hive_username
HIVE_PASSWORD=your_hive_password
HIVE_AUTH=NONE  # 认证方式: NONE, LDAP, KERBEROS, CUSTOM

# 数据库类型选择 (postgresql 或 hive)
DB_TYPE=postgresql

# DeepSeek API配置
DEEPSEEK_API_KEY=your_deepseek_api_key
```

## 使用方法

1. 设置环境变量文件`.env`中的`DB_TYPE`来选择数据库类型（postgresql或hive）
2. 运行主程序：

```bash
python extract_schema.py
```

程序将：
1. 连接到您选定的数据库
2. 提取数据库模式信息并保存为JSON文件
3. 提示您输入一个查询问题
4. 使用DeepSeek AI生成相应的SQL查询语句

## 离线使用

如果只需要提取schema而不生成SQL查询，可以在无网络环境下使用。程序会在尝试调用API失败时继续运行，并且仍然会保存schema信息。

## 示例

### PostgreSQL查询示例

```
查询所有客户的订单数量，按订单数量降序排列，只显示前10个客户
```

### Hive查询示例

```
统计每个部门的员工数量，并按部门名称排序
```

## 注意事项

- 需要有效的DeepSeek API密钥才能使用SQL生成功能
- 生成的SQL查询质量取决于数据库结构的完整性和问题的明确性
- 请在实际执行生成的SQL前仔细检查其正确性
- Hive连接可能需要额外的认证配置，请根据您的Hive部署情况调整 

## 准确率
应用开发前已经设定为用途为单一数据源，或小型数仓（50张表以内），财务数仓场景适用。没有rag,fine tuning,知识库等方式优化LLM性能，完全取决于大模型本身能力，deepseek v3经调研大概有不错的准确性。请仔细甄别

gemini-2.5pro-exp-thinking:关于 DeepSeek V3（特别是 0324 版本）的 通用 SQL 能力，虽然可能缺少专门针对“0324”这个精确版本的公开、标准化基准评测数据，但我们可以基于对 DeepSeek V3 系列以及当前大型语言模型（LLM）在代码生成领域普遍能力的理解，给出一个大致的评估：
通常认为的优势：
基础语法和常见查询：像 DeepSeek V3 这样的现代 LLM 通常能够很好地理解和生成标准的 SQL 语法，包括 SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT 等常见子句，以及基础的 JOIN 操作。
理解 Schema 上下文：对于结构清晰、规模适中（例如您在 README 中提到的 50 表以内）的数据库 Schema，它通常能理解表、列之间的关系，并基于此生成相关的查询。
从自然语言到 SQL：将清晰、明确的自然语言需求转换为 SQL 查询是 LLM 的一个核心能力。对于不涉及极其复杂逻辑的请求，DeepSeek V3 预计能有不错的表现。
潜在的挑战和需要注意的方面：
复杂查询：对于高度复杂的查询，例如涉及多个嵌套子查询、复杂的窗口函数、递归查询 (CTE) 或需要深度逻辑推理的查询，模型的准确率可能会下降。
