import logging
from dq_flow.database import SessionLocal, PipelineLog

class DatabaseLogHandler(logging.Handler):
    """
    Handler customizado que sabe como lidar com o nome do pipeline.
    """
    def __init__(self, pipeline_name="geral"):
        super().__init__()
        self.pipeline_name = pipeline_name

    def emit(self, record):
        session = SessionLocal()
        try:
            log_entry = PipelineLog(
                # Usa o nome do pipeline armazenado no handler
                pipeline_name=self.pipeline_name,
                etapa=getattr(record, 'etapa', 'GERAL'),
                status=getattr(record, 'status', 'INFO'),
                mensagem=record.getMessage(),
                detalhes=getattr(record, 'detalhes', None)
            )
            session.add(log_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"CRITICAL: Falha ao escrever log no banco de dados via ORM: {e}")
        finally:
            session.close()

def get_logger(name, pipeline_name="default_pipeline"):
    """
    Configura e retorna um logger. Agora, aceita um 'pipeline_name'
    para passá-lo ao handler do banco de dados.
    """
    logger = logging.getLogger(f"{name}.{pipeline_name}")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(pipeline_name)s - %(etapa)s] - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Passamos o nome do pipeline ao criar o handler
        db_handler = DatabaseLogHandler(pipeline_name=pipeline_name)
        db_handler.setLevel(logging.INFO)
        logger.addHandler(db_handler)
    
    class ContextFilter(logging.Filter):
        def filter(self, record):
            # Adiciona os campos ao registro para uso no formatter do console
            record.etapa = getattr(record, 'etapa', 'GERAL')
            record.pipeline_name = pipeline_name
            return True
    logger.addFilter(ContextFilter())

    return logger