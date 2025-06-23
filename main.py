import sys
import os

# Adiciona o diretório raiz do projeto ao sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dq_flow.database import SessionLocal, StageCliente, HistoricoCliente
from dq_flow.logger import get_logger
from dq_flow.validation import validate_data_with_pandera
import pandas as pd

PIPELINE_NAME = "clientes_stage_to_hist_orm"

def run():
    # O restante do código permanece exatamente o mesmo
    logger = get_logger(__name__, pipeline_name=PIPELINE_NAME)
    logger.info("Iniciando pipeline com ORM.", extra={'status': 'STARTED'})
    
    session = SessionLocal()
    try:
        # 1. EXTRAÇÃO (usando query ORM)
        logger.info("Extraindo dados da tabela 'stage_clientes' via ORM.")
        
        query = session.query(StageCliente)
        df_stage = pd.read_sql(query.statement, session.bind)
        
        logger.info(f"{len(df_stage)} registros extraídos.")
        if df_stage.empty:
            logger.info("Tabela de stage vazia. Nenhum dado a processar.", extra={'status': 'SUCCESS'})
            return

        # 2. VALIDAÇÃO
        logger.info("Iniciando validação com Pandera.")
        is_valid_pandera, df_validated, errors = validate_data_with_pandera(df_stage.drop(columns=['id'], errors='ignore'))
        
        if not is_valid_pandera:
            error_message = f"Validação Pandera falhou. {len(errors)} registros inválidos."
            logger.error(error_message, extra={'status': 'FAILED_VALIDATION'})
            errors.to_csv('data/quarentena_clientes.csv', index=False)
            logger.warning("Detalhes dos erros salvos em 'data/quarentena_clientes.csv'.")
            return

        logger.info("Validação Pandera concluída com sucesso.")

        # 3. CARGA (Load com ORM)
        logger.info("Iniciando carga para a tabela 'historico_clientes' via ORM.")
        
        registros_historico = [
            HistoricoCliente(**row) for row in df_validated.to_dict(orient='records')
        ]

        session.bulk_save_objects(registros_historico)
        session.commit()
        
        logger.info(f"{len(registros_historico)} registros carregados com sucesso.", extra={'status': 'SUCCESS'})

    except Exception as e:
        logger.error(f"Erro crítico no pipeline: {e}", exc_info=True, extra={'status': 'CRITICAL_FAILURE'})
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    run()