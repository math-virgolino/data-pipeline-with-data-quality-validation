import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dq_flow.database import SessionLocal, StageCliente, HistoricoCliente
from dq_flow.logger import get_logger
from dq_flow.validation import validate_data_with_pandera

# --- DEFININDO O NOME DO PIPELINE ---
PIPELINE_NAME = "clientes_stage_to_hist"
# E passando para o logger
logger = get_logger(__name__, pipeline_name=PIPELINE_NAME)
# -----------------------------------

def run():
    # O resto do código funciona exatamente como antes, pois o logger
    # já sabe o nome do pipeline.
    log_context = {'etapa': 'INICIO', 'status': 'STARTED'}
    logger.info("Iniciando pipeline.", extra=log_context)
    
    session = SessionLocal()
    try:
        # 1. EXTRAÇÃO
        log_context.update({'etapa': 'EXTRACAO'})
        logger.info("Extraindo dados da tabela 'stage_clientes'.", extra=log_context)
        query = session.query(StageCliente)
        df = pd.read_sql(query.statement, session.bind)
        logger.info(f"{len(df)} registros extraídos.", extra=log_context)
        
        # 2. TRANSFORMAÇÃO
        log_context.update({'etapa': 'TRANSFORMACAO'})
        logger.info("Iniciando limpeza e transformação dos dados.", extra=log_context)
        if 'id' in df.columns:
            df = df.drop(columns=['id'])
        df['id_cliente'] = pd.to_numeric(df['id_cliente'].str.extract(r'(\d+)', expand=False), errors='coerce')
        df.loc[df['email'] == 'email_invalido', 'email'] = None
        df_clean = df.dropna(subset=['id_cliente', 'email'])
        logger.info(f"{len(df_clean)} registros permaneceram após limpeza.", extra=log_context)

        # 3. VALIDAÇÃO
        log_context.update({'etapa': 'VALIDACAO'})
        logger.info("Iniciando validação com Pandera.", extra=log_context)
        is_valid, df_validated, errors = validate_data_with_pandera(df_clean)

        if not is_valid:
            log_context.update({'status': 'FAILED', 'detalhes': errors.to_string()})
            logger.error(f"Validação falhou. {len(errors)} registros inválidos.", extra=log_context)
            errors.to_csv('data/quarentena_clientes.csv', index=False)
            logger.warning("Dados reprovados salvos em 'data/quarentena_clientes.csv'.", extra=log_context)
            return

        # 4. CARGA
        log_context.update({'etapa': 'CARGA', 'status': 'RUNNING'})
        logger.info("Iniciando carga para a tabela 'historico_clientes'.", extra=log_context)
        registros_historico = [HistoricoCliente(**row) for row in df_validated.to_dict(orient='records')]
        session.bulk_save_objects(registros_historico)
        session.commit()
        log_context.update({'status': 'SUCCESS'})
        logger.info(f"{len(registros_historico)} registros carregados.", extra=log_context)

    except Exception as e:
        log_context.update({'etapa': 'ERRO_CRITICO', 'status': 'CRITICAL_FAILURE', 'detalhes': str(e)})
        logger.critical(f"Erro inesperado: {e}", exc_info=True, extra=log_context)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    run()