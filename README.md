# Projeto: Pipeline de Data Quality com Python

Este projeto implementa um framework de ETL (Extract, Transform, Load) e Data Quality em Python, projetado para ser modular, reutilizável e agnóstico a banco de dados. Ele orquestra a movimentação de dados de uma camada de *Stage* para uma camada de *Histórico*, aplicando regras de limpeza, transformação e validação de qualidade, com um sistema de logging robusto que registra cada passo em um banco de dados.

### Tecnologias Principais
- **Python 3.9+**
- **Poetry**: Gerenciamento de dependências.
- **SQLAlchemy**: ORM e camada de abstração para comunicação com o banco de dados (suporta SQL Server, PostgreSQL, etc.).
- **Pandas**: Manipulação e transformação de dados em memória.
- **Pandera**: Validação de schema e qualidade de dados de forma estruturada e rigorosa.
- **python-dotenv**: Gerenciamento de variáveis de ambiente e configurações.

### Estrutura do Projeto
```
dq_flow_project/
├── .venv/
├── data/
│   ├── stage_clientes.csv      # Dados fictícios brutos
│   └── quarentena_clientes.csv # Saída de dados que falharam na validação
├── dq_flow/
│   ├── init.py
│   ├── database.py             # Modelos ORM (tabelas), engine e sessão do DB
│   ├── logger.py               # Configuração do logger para console e DB
│   ├── validation.py           # Schemas de validação do Pandera
│   └── pipeline.py             # (Opcional, se a lógica for movida para cá)
├── great_expectations/         # (Opcional, se o framework for adicionado)
├── scripts/
│   ├── generate_fake_data.py   # Script para gerar dados fictícios
│   └── setup_database.py       # Script para criar/resetar as tabelas no DB
├── .env                        # Arquivo de configuração local (NÃO versionar)
├── .gitignore
├── main.py                     # Ponto de entrada para executar o pipeline de clientes
└── pyproject.toml              # Arquivo de configuração do Poetry
```
---

## 1. Escopo e Arquitetura

### Objetivos
- **Automatizar a Ingestão de Dados**: Orquestrar a movimentação de dados entre as camadas do Data Warehouse.
- **Garantir a Qualidade dos Dados**: Implementar uma camada de validação robusta para rejeitar dados que não atendem aos critérios de qualidade definidos.
- **Centralizar o Logging**: Criar um rastro de auditoria detalhado para cada execução do pipeline, registrando cada etapa, sucesso ou falha em uma tabela de banco de dados.
- **Ser Agnóstico a Banco de Dados**: Graças ao SQLAlchemy, a mesma lógica pode ser apontada para diferentes bancos (SQL Server, PostgreSQL, etc.) com mínima alteração na configuração.
- **Ser Extensível**: O design modular permite a fácil adição de novos pipelines para processar outras tabelas com o mínimo de reescrita de código.

### Fluxo do Pipeline (E-T-V-L)
O pipeline principal (`main.py`) segue o fluxo **Extract - Transform - Validate - Load**:

1.  **Extração (Extract)**: Os dados são extraídos da tabela `stage_*` no banco de dados e carregados em um DataFrame Pandas.
2.  **Transformação (Transform)**: Uma série de passos de limpeza são aplicados aos dados. Esta é a etapa crucial onde os dados "sujos" são preparados para a validação (ex: extrair números de strings, tratar valores nulos, etc.).
3.  **Validação (Validate)**: O DataFrame transformado é submetido às regras definidas no schema do `pandera`. Se a validação falhar, o processo é interrompido, e os dados inválidos são movidos para a "quarentena".
4.  **Carga (Load)**: Se a validação for bem-sucedida, os dados limpos e validados são carregados na tabela de destino `historico_*`.

---

## 2. Configurações Iniciais

Siga estes passos para configurar e executar o projeto pela primeira vez.

### Pré-requisitos
- Python 3.9 ou superior instalado.
- Poetry instalado (`pip install poetry`).
- Acesso a um banco de dados SQL Server (ou outro suportado pelo SQLAlchemy).

### Passos da Instalação
1.  **Clone o Repositório**
    ```bash
    git clone <url_do_seu_repositorio>
    cd dq_flow_project
    ```

2.  **Instale as Dependências**
    O Poetry irá criar um ambiente virtual e instalar todas as bibliotecas listadas no `pyproject.toml`.
    ```bash
    poetry install
    ```

3.  **Configure o Ambiente**
    Crie um arquivo `.env` na raiz do projeto para armazenar suas credenciais de banco de dados. Você pode copiar o exemplo.
    ```bash
    # No Windows (CMD)
    copy .env.example .env
    # No Linux/macOS
    cp .env.example .env
    ```
    Agora, **edite o arquivo `.env`** com os dados do seu servidor e banco de dados:
    ```env
    DB_SERVER="SEU_SERVIDOR\INSTANCIA_SQL"
    DB_DATABASE="NOME_DO_BANCO_DE_DADOS"
    ```

4.  **Gere os Dados Fictícios**
    Este passo cria o arquivo `data/stage_clientes.csv` que será usado pelo script de setup.
    ```bash
    poetry run python scripts/generate_fake_data.py
    ```

5.  **Prepare o Banco de Dados**
    Este é um passo **obrigatório**. O script irá apagar (se existirem) e recriar todas as tabelas necessárias (`stage_clientes`, `historico_clientes`, `pipeline_logs`) e carregar os dados fictícios na tabela de stage.
    ```bash
    poetry run python scripts/setup_database.py
    ```
    Após este passo, seu ambiente está pronto.

---

## 3. Como Usar

Com o ambiente configurado, a execução do pipeline é simples.

### Executando o Pipeline
Para rodar o pipeline de ingestão de clientes, execute o `main.py`:
```bash
poetry run python main.py
```

### Verificando os Resultados
Após a execução, você pode verificar os resultados em três lugares:
1.  **Console**: O output do terminal mostrará o progresso do pipeline em tempo real.
2.  **Tabela de Logs**: Consulte a tabela `pipeline_logs` no seu banco de dados para ver o rastro de auditoria detalhado da execução.
    ```sql
    SELECT * FROM pipeline_logs ORDER BY timestamp DESC;
    ```
3.  **Arquivo de Quarentena**: Se houverem dados que falharam na validação, eles estarão no arquivo `data/quarentena_clientes.csv`, prontos para análise.

---

## 4. Como Adicionar Mais Tabelas ou Trocar de Projeto

O framework foi projetado para ser facilmente adaptado para novas fontes de dados (novas tabelas). Imagine que você agora precisa processar uma tabela `stage_produtos`.

Aqui está o guia para criar um novo pipeline de produtos:

#### Passo 1: Adicione o Modelo ORM em `database.py`
Defina as classes para `StageProduto` e `HistoricoProduto` no arquivo `dq_flow/database.py`, assim como fizemos para `Cliente`.

```python
# Em dq_flow/database.py

class StageProduto(Base):
    __tablename__ = 'stage_produtos'
    id = Column(Integer, primary_key=True)
    sku = Column(String(50), nullable=True)
    nome_produto = Column(String(255))
    preco = Column(Numeric(10, 2))
    # ... outras colunas

class HistoricoProduto(Base):
    __tablename__ = 'historico_produtos'
    id = Column(Integer, primary_key=True)
    sku = Column(String(50))
    nome_produto = Column(String(255))
    preco = Column(Numeric(10, 2))
    # ... outras colunas
```
#### Passo 2: Crie o Schema de Validação em `validation.py`
Defina um novo schema `pandera` com as regras de qualidade para os dados de produtos.

```python
# Em dq_flow/validation.py
import pandera.pandas as pa

produto_schema = pa.DataFrameSchema({
    "sku": pa.Column(str, pa.Check.str_startswith("PROD-")),
    "nome_produto": pa.Column(str),
    "preco": pa.Column(pa.Float, pa.Check.gt(0)),
    # ... outras regras
})

def validate_produtos_with_pandera(df: pd.DataFrame):
    """
    Valida um DataFrame de produtos usando o produto_schema.
    """
    try:
        validated_df = produto_schema.validate(df, lazy=True)
        print("Validação de Produtos com Pandera: SUCESSO.")
        return True, validated_df, None
    except pa.errors.SchemaError as err:
        error_report = err.failure_cases
        print("Validação de Produtos com Pandera: FALHA.")
        return False, df, error_report
```

#### Passo 3: Crie um Novo Orquestrador `pipeline_produtos.py`
Crie um novo arquivo na raiz do projeto, como `pipeline_produtos.py`. Ele será muito parecido com o `main.py`, mas focado no novo fluxo.

```python
# Em pipeline_produtos.py
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importe os componentes necessários
from dq_flow.database import SessionLocal, StageProduto, HistoricoProduto
from dq_flow.logger import get_logger
from dq_flow.validation import validate_produtos_with_pandera # Importa a nova função

# Defina o nome do novo pipeline
PIPELINE_NAME = "produtos_stage_to_hist"
logger = get_logger(__name__, pipeline_name=PIPELINE_NAME)

def run_produtos_pipeline():
    """
    Executa o pipeline completo para a tabela de produtos.
    """
    log_context = {'etapa': 'INICIO', 'status': 'STARTED'}
    logger.info("Iniciando pipeline de produtos.", extra=log_context)
    
    session = SessionLocal()
    try:
        # 1. Extrair de 'StageProduto'
        log_context.update({'etapa': 'EXTRACAO'})
        logger.info("Extraindo dados da tabela 'stage_produtos'.", extra=log_context)
        query = session.query(StageProduto)
        df = pd.read_sql(query.statement, session.bind)
        
        # 2. Transformar os dados de produtos (se necessário)
        log_context.update({'etapa': 'TRANSFORMACAO'})
        logger.info("Aplicando transformações nos dados de produtos.", extra=log_context)
        # Adicione aqui a lógica de limpeza específica para produtos
        df_clean = df 

        # 3. Validar usando 'validate_produtos_with_pandera'
        log_context.update({'etapa': 'VALIDACAO'})
        logger.info("Iniciando validação com Pandera.", extra=log_context)
        is_valid, df_validated, errors = validate_produtos_with_pandera(df_clean)
        
        if not is_valid:
            # ... (lógica de tratamento de falha na validação) ...
            return

        # 4. Carregar em 'HistoricoProduto'
        log_context.update({'etapa': 'CARGA'})
        logger.info("Iniciando carga para a tabela 'historico_produtos'.", extra=log_context)
        # ... (lógica de carga para a tabela de histórico) ...

        log_context.update({'status': 'SUCCESS'})
        logger.info("Pipeline de produtos concluído com sucesso.", extra=log_context)

    except Exception as e:
        # ... (lógica de tratamento de erro) ...
        pass
    finally:
        session.close()


if __name__ == "__main__":
    run_produtos_pipeline()
```

**A chave aqui é a reutilização**: o get_logger e a estrutura geral são os mesmos, você apenas troca os modelos, as transformações e a função de validação.

#### Passo 4: Adapte os Scripts de Setup

- Crie um `generate_fake_produtos.py` para gerar dados de teste para os produtos.
- Atualize o `setup_database.py` para que ele também crie e popule a tabela `stage_produtos`. O comando `Base.metadata.create_all(engine)` já fará isso automaticamente se você adicionou as novas classes de modelo no arquivo database.py.