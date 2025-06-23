import logging
from dq_flow.database import SessionLocal, PipelineLog

class DatabaseLogHandler(logging.Handler):
    def emit(self, record):
        session = SessionLocal()
        try:
            log_entry = PipelineLog(
                pipeline_name=getattr(record, 'pipeline_name', 'N/A'),
                log_level=record.levelname,
                message=record.getMessage(),
                status=getattr(record, 'status', 'INFO')
            )
            session.add(log_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"CRITICAL: Falha ao escrever log no banco de dados via ORM: {e}")
        finally:
            session.close()

def get_logger(name, pipeline_name="default_pipeline"):
    # Esta função não precisa de alterações
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        db_handler = DatabaseLogHandler()
        db_handler.setLevel(logging.INFO)
        logger.addHandler(db_handler)
    adapter = logging.LoggerAdapter(logger, {'pipeline_name': pipeline_name})
    return adapter