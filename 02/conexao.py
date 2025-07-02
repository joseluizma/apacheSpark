from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Verifique se o driver JDBC existe
jdbc_driver_path = "/home/repository/spark/02/postgresql-42.7.6.jar"
if not os.path.exists(jdbc_driver_path):
    raise FileNotFoundError(f"Driver JDBC não encontrado em: {jdbc_driver_path}")

# Configurar a sessão Spark
spark = SparkSession.builder \
    .appName("PostgreSQL") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

# Obter parâmetros de conexão do arquivo .env
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Validar se todas as variáveis foram carregadas
if not all([db_host, db_port, db_name, db_user, db_password]):
    raise ValueError("Uma ou mais variáveis de ambiente não foram definidas no arquivo .env")

# Construir a URL JDBC
url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Definir as propriedades de conexão
properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

try:
    # Teste a conexão com uma query simples
    df_test = spark.read.jdbc(url=url, table="(SELECT 1) AS test", properties=properties)
    print("Conexão com o banco de dados bem-sucedida!")
    
    # Carregue os dados da tabela
    df = spark.read.jdbc(url=url, table="usu_0.t101", properties=properties)
    
    # Imprima o schema e os dados
    df.printSchema()
    df.show()

except Exception as e:
    print(f"Erro ao conectar ou ler a tabela: {str(e)}")

finally:
    # Feche a sessão Spark
    spark.stop()