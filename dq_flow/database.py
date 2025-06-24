import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, Date, Text, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func
import urllib


# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Base declarativa para os modelos ORM
Base = declarative_base()

def get_db_engine():
    """
    Cria e retorna a engine de conexão com o SQL Server,
    otimizada para cargas de dados em massa.
    """
    db_server = os.getenv("DB_SERVER")
    db_database = os.getenv("DB_DATABASE")
    
    # Validação para garantir que as variáveis de ambiente foram carregadas
    if not all([db_server, db_database]):
        raise ValueError("ERRO: As variáveis de ambiente DB_SERVER e DB_DATABASE devem ser definidas.")

    # A string de conexão foi mantida, pois está correta para Autenticação do Windows.
    # O uso do urllib.parse.quote_plus é uma boa prática para evitar problemas com caracteres especiais.
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_database};"
        "Trusted_Connection=yes;"
    )
    
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    
    try:
        # ---- AJUSTE PRINCIPAL ----
        # Adicionado o parâmetro `fast_executemany=True` na criação da engine.
        # Isso otimiza drasticamente as operações de inserção em massa.
        engine = create_engine(conn_str, fast_executemany=True)
        
        # O teste de conexão é uma excelente prática.
        with engine.connect() as connection:
            print(f"Conexão com '{db_server}\\{db_database}' estabelecida com sucesso.")
            print(f"Modo fast_executemany: Habilitado")
        return engine
    
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        # 'raise' sem argumentos dentro de um except irá relançar a exceção original,
        # preservando o stack trace, o que é ótimo para debugging.
        raise

# Cria a engine que será usada em toda a aplicação
engine = get_db_engine()

# Cria uma fábrica de sessões configurada
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Modelos ORM (Definição das Tabelas) ---

class StageCliente(Base):
    """Modelo para a tabela de staging de clientes."""
    __tablename__ = 'stage_clientes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_cliente = Column(String(255))
    nome = Column(String(255))
    email = Column(String(255))
    data_cadastro = Column(Date)
    valor_ultima_compra = Column(Numeric(10, 2)) # Precisão correta
    status = Column(String(255))

class HistoricoCliente(Base):
    """Modelo para a tabela de histórico de clientes validados."""
    __tablename__ = 'historico_clientes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_cliente = Column(String(50), index=True)
    nome = Column(String(255))
    email = Column(String(255), unique=True)
    data_cadastro = Column(Date)
    valor_ultima_compra = Column(Numeric(10, 2)) # Precisão correta
    status = Column(String(50))
    data_insercao = Column(DateTime, server_default=func.now())

class PipelineLog(Base):
    __tablename__ = 'pipeline_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=func.now())
    pipeline_name = Column(String(255), nullable=False, default="N/A")    
    etapa = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    mensagem = Column(Text)
    detalhes = Column(Text, nullable=True)

    def __repr__(self):
        # Atualizando o __repr__ para ser mais informativo
        return f"<PipelineLog(pipeline='{self.pipeline_name}', etapa='{self.etapa}', status='{self.status}')>"

def create_all_tables():
    """Cria todas as tabelas no banco de dados que não existirem."""
    print("Verificando e criando tabelas a partir dos modelos ORM...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Tabelas criadas/verificadas com sucesso.")
    except Exception as e:
        print(f"ERRO ao criar as tabelas: {e}")
        raise