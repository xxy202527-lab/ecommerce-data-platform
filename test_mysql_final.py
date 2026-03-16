import pymysql
import sqlalchemy as sa
import pandas as pd


def test_mysql():
    """完整的MySQL测试"""

    # 1. 测试连接
    print("=" * 50)
    print("测试MySQL连接...")
    print("=" * 50)

    try:
        # pymysql连接测试
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root123',  # 换成你的密码
            database='mysql'
        )
        print("✅ pymysql连接成功")

        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"   MySQL版本: {version[0]}")
        conn.close()

        # SQLAlchemy连接测试
        engine = sa.create_engine(
            'mysql+pymysql://root:root123@localhost:3306/ecommerce_dw'
        )

        with engine.connect() as connection:
            result = connection.execute(sa.text("SELECT DATABASE()"))
            db_name = result.fetchone()[0]
            print(f"✅ SQLAlchemy连接成功")
            print(f"   当前数据库: {db_name}")

        # 创建测试表
        print("\n创建测试表...")
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['产品A', '产品B', '产品C'],
            'price': [99.9, 199.9, 299.9],
            'create_time': pd.Timestamp.now()
        })

        test_df.to_sql(
            'test_products',
            engine,
            if_exists='replace',
            index=False
        )
        print("✅ 测试表创建成功")

        # 读取测试数据
        result = pd.read_sql("SELECT * FROM test_products", engine)
        print(f"\n读取到 {len(result)} 条数据:")
        print(result)

        # 清理测试表
        with engine.connect() as conn:
            conn.execute(sa.text("DROP TABLE test_products"))
            conn.commit()
        print("\n✅ 测试表已清理")

        print("\n" + "=" * 50)
        print("🎉 所有测试通过！MySQL环境准备就绪！")
        print("=" * 50)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        return False

    return True


if __name__ == "__main__":
    test_mysql()