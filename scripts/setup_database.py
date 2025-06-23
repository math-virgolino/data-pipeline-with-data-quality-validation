import sys
import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Adiciona a raiz do projeto ao path para encontrar o módulo 'dq_flow'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

# Importamos os componentes necessários. Note que 'SessionLocal' não é mais usado aqui.
from dq_flow.database import StageCliente, create_all_tables, Base, engine

def run_setup():
    """
    Reseta o banco, carrega dados do CSV e os insere usando SQLAlchemy Core.
    """
    # 1. ETAPA DE PREPARAÇÃO DO BANCO (sem alterações)
    try:
        print("Iniciando setup e RESET COMPLETO do banco de dados...")
        print("Etapa: Apagando tabelas antigas (se existirem)...")
        Base.metadata.drop_all(bind=engine)
        print("Etapa: Tabelas antigas removidas com sucesso.")

        print("Etapa: Recriando tabelas a partir dos modelos Python...")
        create_all_tables()

        print("Etapa: Carregando dados do CSV...")
        csv_path = os.path.join(PROJECT_ROOT, 'data', 'stage_clientes.csv')
        df_fake = pd.read_csv(csv_path, delimiter=',')

    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em '{csv_path}'.")
        return
    except Exception as e:
        print(f"ERRO na fase de inicialização do banco: {e}")
        return

    # 2. ETAPA DE PROCESSAMENTO DOS DADOS (adaptada)
    # Preparamos uma lista de dicionários, em vez de objetos StageCliente.
    lista_de_dicionarios = []
    print("Iniciando processamento e conversão dos dados do CSV...")
    for index, row in df_fake.iterrows():
        row_data = row.to_dict()
        try:
            valor_original = row_data['valor_ultima_compra']
            valor_decimal = Decimal(str(valor_original)).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            lista_de_dicionarios.append(row_data)
        except Exception as conversion_error:
            print(f"ERRO ao converter a linha {index}: {row_data}")
            print(f"--> Erro específico: {conversion_error}")
            raise

    # 3. ETAPA DE INSERÇÃO (reescrita com SQLAlchemy Core)
    # Verificamos se há dados a serem inseridos antes de conectar.
    if not lista_de_dicionarios:
        print("Nenhum dado válido para inserir. Finalizando.")
        return

    print(f"Etapa: Iniciando BULK INSERT (via SQLAlchemy Core) de {len(lista_de_dicionarios)} registros...")
    
    # Usamos uma conexão direta da engine, sem usar a Session do ORM.
    with engine.connect() as connection:
        try:
            # Pegamos a referência da tabela diretamente do modelo ORM
            tabela = StageCliente.__table__
            
            # Executamos o 'insert' em massa passando a lista de dicionários
            connection.execute(tabela.insert(), lista_de_dicionarios)
            
            # Finalizamos a transação com um commit explícito
            connection.commit()
            
            print("Etapa: BULK INSERT (via Core) finalizado com sucesso.")
            print(f"\nSUCESSO: {len(lista_de_dicionarios)} registros carregados em 'stage_clientes'.")
        
        except Exception as e:
            print(f"\nERRO FATAL durante a carga de dados via Core: {e}")
            # A transação é revertida automaticamente ao sair do bloco 'with' em caso de erro.

if __name__ == "__main__":
    run_setup()