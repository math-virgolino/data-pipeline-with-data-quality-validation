import sys
import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Adiciona a raiz do projeto ao path para encontrar o módulo 'dq_flow'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

# Importamos os componentes necessários.
from dq_flow.database import StageCliente, create_all_tables, Base, engine

def run_setup_otimizado():
    """
    Reseta o banco, carrega dados do CSV e os insere usando processamento
    vetorizado do Pandas e SQLAlchemy Core.
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
        df = pd.read_csv(csv_path, delimiter=',')
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], format='%d/%m/%Y', errors='coerce')
        df['valor_ultima_compra'] = df['valor_ultima_compra'].astype(float)
        df['id_cliente'] = df['id_cliente'].astype(str)
        df['nome'] = df['nome'].astype(str)
        df['email'] = df['email'].astype(str)
        df['status'] = df['status'].astype(str)

    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em '{csv_path}'.")
        return
    except Exception as e:
        print(f"ERRO na fase de inicialização do banco: {e}")
        return

    # 2. ETAPA DE PROCESSAMENTO VETORIZADO (Otimização Principal)
    print("Iniciando processamento vetorizado dos dados do DataFrame...")
    try:
        # ---- INÍCIO DA OTIMIZAÇÃO ----

        # 2.1. Define uma função para a conversão, garantindo o tratamento de nulos.
        def converter_para_decimal(valor):
            if pd.isna(valor):
                return None  # Mantém valores nulos como None (SQL NULL)
            # A conversão via string é mais segura para evitar imprecisões de float
            return Decimal(str(valor)).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )

        # 2.2. Aplica a função à coluna inteira de uma só vez (vetorização).
        # Isto é ordens de magnitude mais rápido que um loop for.
        df['valor_ultima_compra'] = df['valor_ultima_compra'].apply(converter_para_decimal)

        # 2.3. Converte o DataFrame processado para uma lista de dicionários.
        # Substitui todo o loop `for` anterior por uma única linha.
        dados_para_inserir = df.to_dict(orient='records')

        # ---- FIM DA OTIMIZAÇÃO ----

    except Exception as e:
        print(f"ERRO ao processar os dados do DataFrame: {e}")
        return

    # 3. ETAPA DE INSERÇÃO (código original mantido, pois já é o ideal)
    if not dados_para_inserir:
        print("Nenhum dado válido para inserir. Finalizando.")
        return

    print(f"Etapa: Iniciando BULK INSERT de {len(dados_para_inserir)} registros...")
    
    # O bloco `with engine.begin() as connection:` é uma forma idiomática e segura
    # que inicia uma transação e faz commit ao final, ou rollback em caso de erro.
    with engine.begin() as connection:
        try:
            tabela = StageCliente.__table__
            connection.execute(tabela.insert(), dados_para_inserir)
            print("Etapa: BULK INSERT finalizado e transação commitada.")
            
        except Exception as e:
            print(f"\nERRO FATAL durante a carga de dados: {e}")
            # O rollback é automático ao sair do bloco 'with' com uma exceção.
            # O 'raise' garante que o erro interrompa o script, sinalizando a falha.
            raise 

    print(f"\nSUCESSO: {len(dados_para_inserir)} registros carregados em 'stage_clientes'.")


if __name__ == "__main__":
    run_setup_otimizado()