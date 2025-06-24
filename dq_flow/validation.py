import pandas as pd
# --- NOVO IMPORT RECOMENDADO ---
import pandera.pandas as pa
# -----------------------------
from pandera.errors import SchemaError

client_schema = pa.DataFrameSchema({
    "id_cliente": pa.Column(int, pa.Check.gt(0), coerce=True, required=True),
    "nome": pa.Column(str, required=True),
    "email": pa.Column(str, pa.Check.str_contains('@'), required=True),
    "data_cadastro": pa.Column(pa.DateTime, coerce=True, required=True),
    # Corrigindo o tipo de dado esperado para corresponder ao modelo do banco
    "valor_ultima_compra": pa.Column(pa.Float, pa.Check.ge(0), coerce=True, required=True),
    "status": pa.Column(str, pa.Check.isin(['ATIVO', 'INATIVO', 'PENDENTE', 'BLOQUEADO']), required=True)
})

def validate_data_with_pandera(df: pd.DataFrame):
    try:
        validated_df = client_schema.validate(df, lazy=True)
        print("Validação com Pandera: SUCESSO.")
        return True, validated_df, None
    except SchemaError as err:
        error_report = err.failure_cases
        print("Validação com Pandera: FALHA.")
        return False, df, error_report