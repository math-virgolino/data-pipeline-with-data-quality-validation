import logging
import json # Importamos a biblioteca JSON
from dq_flow.database import SessionLocal, PipelineLog

class DatabaseLogHandler(logging.Handler):
    def __init__(self, pipeline_name="geral"):
        super().__init__()
        self.pipeline_name = pipeline_name

    def emit(self, record):
        session = SessionLocal()
        try:
            # Pegamos os detalhes do registro.
            detalhes = getattr(record, 'detalhes', None)

            # --- NOVA LÓGICA DE SERIALIZAÇÃO ---
            # Se os detalhes forem um dicionário, converte para uma string JSON formatada.
            # Caso contrário, apenas converte para string.
            if isinstance(detalhes, dict):
                detalhes_str = json.dumps(detalhes, indent=4, ensure_ascii=False)
            else:
                detalhes_str = str(detalhes) if detalhes is not None else None
            # --- FIM DA NOVA LÓGICA ---

            log_entry = PipelineLog(
                pipeline_name=self.pipeline_name,
                etapa=getattr(record, 'etapa', 'GERAL'),
                status=getattr(record, 'status', 'INFO'),
                mensagem=record.getMessage(), # A mensagem estática
                detalhes=detalhes_str # Os detalhes (agora como string JSON)
            )
            session.add(log_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"CRITICAL: Falha ao escrever log no banco de dados via ORM: {e}")
        finally:
            session.close()

# A função get_logger não precisa de alterações.
def get_logger(name, pipeline_name="default_pipeline"):
    # ... (código existente)
    logger = logging.getLogger(f"{name}.{pipeline_name}")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(pipeline_name)s - %(etapa)s] - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        db_handler = DatabaseLogHandler(pipeline_name=pipeline_name)
        db_handler.setLevel(logging.INFO)
        logger.addHandler(db_handler)
    
    class ContextFilter(logging.Filter):
        def filter(self, record):
            record.etapa = getattr(record, 'etapa', 'GERAL')
            record.pipeline_name = pipeline_name
            return True
    logger.addFilter(ContextFilter())

    return logger