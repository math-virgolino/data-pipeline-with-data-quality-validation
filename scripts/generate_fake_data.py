import pandas as pd
from faker import Faker
import random
import os

# Inicializa o Faker para o idioma português do Brasil
fake = Faker('pt_BR')

def create_fake_clients_corrected(n=100):
    """
    Gera um DataFrame de clientes falsos e o salva como um arquivo CSV.
    Os valores numéricos são gerados dentro de um range seguro para numeric(10, 2).
    """
    data = []
    status_options = ['ATIVO', 'INATIVO', 'PENDENTE', 'BLOQUEADO']

    print(f"Gerando {n} registros de clientes falsos...")

    for i in range(n):
        # --- Bloco 1: Gera dados propositalmente "ruins" para testes de ETL ---
        if i % 10 == 0:
            record = {
                'id_cliente': f"CLI_{i+1000}",
                'nome': f"Sr(a). {fake.name()}",
                'email': 'email_invalido',
                'data_cadastro': fake.date_between(start_date='-5y', end_date='today'),
                'valor_ultima_compra': -150.50,
                'status': 'INVALIDO'
            }
        # --- Bloco 2: Gera dados com campos nulos ---
        elif i % 15 == 0:
            record = {
                'id_cliente': None,
                'nome': fake.name(),
                'email': None,
                'data_cadastro': fake.date_between(start_date='-5y', end_date='today'),
                'valor_ultima_compra': round(random.uniform(10.0, 5000.0), 2),
                'status': random.choice(status_options)
            }
        # --- Bloco 3: Gera dados "bons" e válidos ---
        else:
            record = {
                'id_cliente': i,
                'nome': fake.name(),
                'email': fake.email(),
                'data_cadastro': fake.date_between(start_date='-5y', end_date='today'),
                'valor_ultima_compra': round(random.uniform(10.0, 9999.0), 2),
                'status': random.choice(status_options)
            }
        data.append(record)
    
    # Cria o DataFrame a partir da lista de dicionários
    df = pd.DataFrame(data)

    # 1. Garante que a coluna seja interpretada como do tipo data.
    #    Isso resolve o AttributeError, pois agora o .dt pode ser usado.
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])

    # 2. Agora que é uma data, formata para o padrão string 'DD/MM/YYYY' para salvar no CSV.
    df['data_cadastro'] = df['data_cadastro'].dt.strftime('%d/%m/%Y')

    # Define o caminho do arquivo e garante que o diretório exista
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, 'stage_clientes.csv')
    
    # Salva o DataFrame como CSV, usando TAB como separador para evitar problemas com vírgulas
    df.to_csv(csv_path, index=False, sep=',', encoding='utf-8')
    
    print(f"Arquivo '{csv_path}' gerado com sucesso com dados novos e corrigidos.")

if __name__ == "__main__":
    create_fake_clients_corrected(100)