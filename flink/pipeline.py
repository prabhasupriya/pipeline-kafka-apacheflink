import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def build_pipeline():
    print("🚀 MNC Pipeline: Compiling Job Graph on Cluster...")
    
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    # Standard environment definition for native cluster running
    exec_env = StreamExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(exec_env, settings)
    
    t_env.get_config().set("pipeline.name", "MNC-RealTime-Feature-Engineering-V1")
    print("✅ Table Environment Initialized Successfully!")

    # --- DATABASES & TABLES DEFINITION ---
    
    # 1. SOURCE: User Events
    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            user_id STRING,
            content_id STRING,
            event_type STRING,
            dwell_time_ms INT,
            ts_str STRING,
            row_time AS TO_TIMESTAMP(ts_str, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
            WATERMARK FOR row_time AS row_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'properties.group.id' = 'flink-feature-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. SOURCE: Content Metadata
    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id STRING,
            category STRING,
            creator_id STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'content-metadata',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 3. SINK: Unified Feature Store
    t_env.execute_sql(f"""
        CREATE TABLE feature_store (
            entity_id STRING,
            feature_name STRING,
            feature_value STRING,
            computed_at STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'feature-store',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'format' = 'json'
        )
    """)

    # --- FEATURE LOGIC ---
    user_sql = """
        INSERT INTO feature_store
        SELECT user_id, 'user_avg_dwell', 
        CAST(AVG(dwell_time_ms) AS STRING),
        DATE_FORMAT(MAX(row_time), 'yyyy-MM-dd HH:mm:ss')
        FROM user_events GROUP BY user_id, TUMBLE(row_time, INTERVAL '1' MINUTE)
    """

    content_sql = """
        INSERT INTO feature_store
        SELECT content_id, 'content_pop_score', 
        CAST(COUNT(event_type) AS STRING),
        DATE_FORMAT(MAX(row_time), 'yyyy-MM-dd HH:mm:ss')
        FROM user_events GROUP BY content_id, HOP(row_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)
    """

    cross_sql = """
        INSERT INTO feature_store
        SELECT e.user_id, 'last_category', 
        m.category,
        DATE_FORMAT(MAX(e.row_time), 'yyyy-MM-dd HH:mm:ss')
        FROM user_events e
        JOIN content_metadata m ON e.content_id = m.content_id
        GROUP BY e.user_id, m.category, TUMBLE(e.row_time, INTERVAL '1' MINUTE)
    """

    print("MNC Pipeline: Packaging queries into execution framework...")
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(user_sql)
    stmt_set.add_insert_sql(content_sql)
    stmt_set.add_insert_sql(cross_sql)
    
    print("🚀 Deploying topology to active stream nodes...")
    try:
        table_result = stmt_set.execute()
        print("✅ SUCCESS! Job deployed to Flink cluster.")
    except Exception as e:
        print(f"Submission Error: {e}")

if __name__ == '__main__':
    build_pipeline()