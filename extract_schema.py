import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# 数据库客户端导入
import psycopg2
try:
    from pyhive import hive
    HIVE_AVAILABLE = True
except ImportError:
    HIVE_AVAILABLE = False

def load_config():
    """加载数据库配置"""
    load_dotenv()
    
    db_type = os.getenv('DB_TYPE', 'postgresql').lower()
    
    if db_type == 'postgresql':
        return {
            'type': 'postgresql',
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432'),
            'client_encoding': 'utf8'  # 添加客户端编码设置
        }
    elif db_type == 'hive':
        auth = os.getenv('HIVE_AUTH', 'NONE').upper()
        config = {
            'type': 'hive',
            'host': os.getenv('HIVE_HOST'),
            'database': os.getenv('HIVE_DATABASE', 'default'),
            'port': os.getenv('HIVE_PORT', '10000'),
            'auth': auth
        }
        
        if auth != 'NONE':
            config['username'] = os.getenv('HIVE_USER')
            config['password'] = os.getenv('HIVE_PASSWORD')
        
        return config
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")

def create_connection(config):
    """创建数据库连接"""
    db_type = config.get('type')
    
    try:
        if db_type == 'postgresql':
            # 移除type键，它不是psycopg2的参数
            pg_config = {k: v for k, v in config.items() if k != 'type'}
            return psycopg2.connect(**pg_config)
        elif db_type == 'hive':
            if not HIVE_AVAILABLE:
                raise ImportError("缺少Hive依赖，请安装: pip install pyhive[hive] thrift thrift-sasl sasl")
            
            auth = config.get('auth')
            hive_config = {
                'host': config.get('host'),
                'port': int(config.get('port')),
                'database': config.get('database')
            }
            
            if auth != 'NONE':
                hive_config['username'] = config.get('username')
                hive_config['password'] = config.get('password')
                hive_config['auth'] = auth
            
            return hive.Connection(**hive_config)
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    except Exception as e:
        print(f"连接失败: {e}")
        exit(1)

def extract_schema(conn, config):
    """提取schema信息"""
    db_type = config.get('type')
    
    if db_type == 'postgresql':
        return extract_postgres_schema(conn)
    elif db_type == 'hive':
        return extract_hive_schema(conn)
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")

def extract_postgres_schema(conn):
    """提取PostgreSQL的schema信息"""
    schema = {'tables': [], 'db_type': 'postgresql'}
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, table_schema 
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
        
        for table in cur.fetchall():
            table_info = {
                'name': table[0],
                'schema': table[1],
                'columns': [],
                'constraints': []
            }
            
            # 获取列信息
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """, (table[0], table[1]))
            
            for col in cur.fetchall():
                table_info['columns'].append({
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] == 'YES',
                    'default': col[3]
                })
            
            # 获取约束信息
            cur.execute("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = %s AND table_schema = %s
            """, (table[0], table[1]))
            
            for cons in cur.fetchall():
                table_info['constraints'].append({
                    'name': cons[0],
                    'type': cons[1]
                })
            
            schema['tables'].append(table_info)
    
    return schema

def extract_hive_schema(conn):
    """提取Hive的schema信息"""
    schema = {'tables': [], 'db_type': 'hive'}
    
    with conn.cursor() as cur:
        # 获取当前数据库的所有表
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        
        for table_name in tables:
            table_info = {
                'name': table_name,
                'schema': conn.database,  # Hive中的database相当于schema
                'columns': []
            }
            
            # 获取表的列信息
            cur.execute(f"DESCRIBE {table_name}")
            for col in cur.fetchall():
                # Hive的DESCRIBE返回格式: name, type, comment
                if len(col) >= 2:
                    name, col_type = col[0], col[1]
                    comment = col[2] if len(col) > 2 else None
                    
                    table_info['columns'].append({
                        'name': name,
                        'type': col_type,
                        'nullable': True,  # Hive默认允许NULL
                        'comment': comment
                    })
            
            # 获取表的分区信息
            try:
                cur.execute(f"SHOW PARTITIONS {table_name}")
                partitions = cur.fetchall()
                if partitions:
                    table_info['partitions'] = [p[0] for p in partitions]
            except:
                # 表可能没有分区
                pass
            
            schema['tables'].append(table_info)
    
    return schema

def save_schema(schema, output_dir=None):
    """保存schema到文件"""
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
    
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{schema.get('db_type', 'db')}_schema.json"
    file_path = os.path.join(output_dir, filename)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    
    return file_path

def generate_sql_prompt(schema, question):
    """根据schema和问题生成提示词"""
    db_type = schema.get('db_type', 'postgresql')
    
    prompt = f"""你是一个{db_type.upper()}专家，请根据以下数据库结构和用户问题，生成合适的SQL查询语句。

## 要求
1. 只返回SQL代码和SQL注释（如--注释内容 或 /* 注释内容 */）
2. 不要在SQL以外添加任何解释或说明
3. 不要使用markdown格式如```sql
4. 不要添加"SQL查询："或类似的前缀
5. 返回的SQL应当可以直接在{db_type.upper()}中执行
6. 可以添加有助于理解SQL逻辑的注释

数据库结构:
"""
    
    for table in schema['tables']:
        prompt += f"\n表名: {table['name']} (Schema: {table.get('schema', 'default')})\n"
        prompt += "列:\n"
        
        for column in table['columns']:
            nullable = "NULL" if column.get('nullable', True) else "NOT NULL"
            default = f"DEFAULT {column.get('default')}" if column.get('default') else ""
            comment = f"COMMENT '{column.get('comment')}'" if column.get('comment') else ""
            prompt += f"  - {column['name']} {column['type']} {nullable} {default} {comment}\n"
        
        if 'constraints' in table and table['constraints']:
            prompt += "约束:\n"
            for constraint in table['constraints']:
                prompt += f"  - {constraint['name']} ({constraint['type']})\n"
        
        if 'partitions' in table and table['partitions']:
            prompt += "分区:\n"
            for partition in table['partitions']:
                prompt += f"  - {partition}\n"
        
        prompt += "\n"
    
    prompt += f"\n用户问题: {question}\n"
    prompt += f"\n请生成适用于{db_type.upper()}的SQL查询语句:"
    
    return prompt

def call_deepseek_api(prompt):
    """调用DeepSeek API生成SQL查询"""
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        raise ValueError("未设置DeepSeek API密钥，请在.env文件中配置DEEPSEEK_API_KEY")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "deepseek-coder",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 2000
    }
    
    response = requests.post(
        "https://api.deepseek.com/v1/chat/completions",
        headers=headers,
        json=data
    )
    
    if response.status_code != 200:
        raise Exception(f"API调用失败: {response.status_code} {response.text}")
    
    result = response.json()["choices"][0]["message"]["content"].strip()
    
    # 处理结果，提取SQL代码
    # 如果结果包含markdown格式的SQL代码块，提取其中的内容
    if result.startswith("```sql") and result.endswith("```"):
        result = result[6:-3].strip()
    elif result.startswith("```") and result.endswith("```"):
        result = result[3:-3].strip()
    
    # 保留SQL代码和SQL注释，去除其他解释性文本
    lines = result.split("\n")
    sql_lines = []
    in_sql_block = False
    
    for line in lines:
        line_stripped = line.strip()
        # 判断是否是SQL代码或SQL注释
        is_sql_comment = line_stripped.startswith("--") or line_stripped.startswith("/*") or line_stripped.endswith("*/") or "/*" in line_stripped
        is_sql_code = len(line_stripped) > 0 and not line_stripped.startswith("#") and not (line_stripped.startswith("SQL") or "解释" in line_stripped or "说明" in line_stripped)
        
        # 收集SQL代码和注释
        if is_sql_code or is_sql_comment:
            sql_lines.append(line)
            in_sql_block = True
        # 空行处理：在SQL块内部的空行保留，用于代码格式
        elif in_sql_block and line_stripped == "":
            sql_lines.append(line)
    
    return "\n".join(sql_lines)

def generate_sql_query(schema, question):
    """根据数据库结构和问题生成SQL查询"""
    prompt = generate_sql_prompt(schema, question)
    return call_deepseek_api(prompt)

if __name__ == '__main__':
    config = load_config()
    conn = create_connection(config)
    schema = extract_schema(conn, config)
    schema_file = save_schema(schema)
    print(f"数据库结构已保存到: {schema_file}")
    
    # 示例：使用DeepSeek生成SQL查询
    try:
        question = input("\n请输入您的查询问题: ")
        if question:
            print("\n正在生成SQL查询...")
            sql_query = generate_sql_query(schema, question)
            print("\n生成的SQL查询:")
            print(sql_query)
    except Exception as e:
        print(f"\n生成SQL查询时出错: {e}")
    
    conn.close()