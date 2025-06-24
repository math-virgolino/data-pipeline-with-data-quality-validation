import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dq_flow.database import SessionLocal, StageCliente, HistoricoCliente
from dq_flow.logger import get_logger
from dq_flow.validation import validate_data_with_pandera

PIPELINE_NAME = "clientes_stage_to_hist"
logger = get_logger(__name__, pipeline_name=PIPELINE_NAME)

def run():
    # Mensagem estática
    msg = "Pipeline iniciado."
    # Detalhes dinâmicos como um dicionário
    detalhes = {"versao_pipeline": "1.1.0"}
    log_context = {'etapa': 'INICIO', 'status': 'STARTED', 'detalhes': detalhes}
    logger.info(msg, extra=log_context)
    
    session = SessionLocal()
    try:
        # 1. EXTRAÇÃO
        log_context.update({'etapa': 'EXTRACAO', 'status': 'RUNNING'})
        logger.info("Extraindo dados da fonte.", extra=log_context)
        
        tabela_origem = StageCliente.__tablename__
        query = session.query(StageCliente)
        df = pd.read_sql(query.statement, session.bind)
        
        log_context.update({
            'detalhes': {"registros_extraidos": len(df), "tabela_origem": tabela_origem}
        })
        logger.info("Extração concluída.", extra=log_context)
        
        # 2. TRANSFORMAÇÃO
        log_context.update({'etapa': 'TRANSFORMACAO'})
        registros_antes = len(df)

        if 'id' in df.columns:
            df = df.drop(columns=['id'])
        df['id_cliente'] = pd.to_numeric(df['id_cliente'].str.extract(r'(\d+)', expand=False), errors='coerce')
        df.loc[df['email'] == 'email_invalido', 'email'] = None
        df_clean = df.dropna(subset=['id_cliente', 'email'])
        
        log_context.update({
            'detalhes': {"registros_antes": registros_antes, "registros_apos": len(df_clean)}
        })
        logger.info("Limpeza e transformação concluídas.", extra=log_context)

        # 3. VALIDAÇÃO
        log_context.update({'etapa': 'VALIDACAO'})
        is_valid, df_validated, errors = validate_data_with_pandera(df_clean)

        if not is_valid:
            log_context.update({
                'status': 'FAILED', 
                'detalhes': {
                    "registros_invalidos": len(errors),
                    "amostra_erros": errors.head().to_dict()
                }
            })
            logger.error("Dados reprovados na validação de qualidade.", extra=log_context)
            errors.to_csv('data/quarentena_clientes.csv', index=False)
            return

        logger.info("Validação de dados concluída com sucesso.", extra=log_context)

        # 4. CARGA
        log_context.update({'etapa': 'CARGA', 'status': 'RUNNING'})
        tabela_destino = HistoricoCliente.__tablename__
        
        registros_historico = [HistoricoCliente(**row) for row in df_validated.to_dict(orient='records')]
        session.bulk_save_objects(registros_historico)
        session.commit()
        
        log_context.update({
            'status': 'SUCCESS',
            'detalhes': {"registros_carregados": len(registros_historico), "tabela_destino": tabela_destino}
        })
        logger.info("Carga de dados na camada Histórico concluída.", extra=log_context)

    except Exception as e:
        log_context.update({'etapa': 'ERRO_CRITICO', 'status': 'CRITICAL_FAILURE', 'detalhes': str(e)})
        logger.critical("Erro inesperado no pipeline.", exc_info=True, extra=log_context)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    run()