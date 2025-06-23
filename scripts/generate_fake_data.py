import pandas as pd
from faker import Faker
import random

fake = Faker('pt_BR')

def create_fake_clients(n=100):
    data = []
    for i in range(n):
        status_options = ['ATIVO', 'INATIVO', 'PENDENTE', 'BLOQUEADO']
        # Introduzir dados "ruins" propositalmente
        if i % 10 == 0: # 10% de dados com problemas
            data.append({
                'id_cliente': f"CLI_{i+1000}", # ID fora do padrão
                'nome': fake.name(),
                'email': 'email_invalido',
                'data_cadastro': fake.date_this_decade(),
                'valor_ultima_compra': -150.50, # Valor negativo
                'status': 'INVALIDO' # Status inválido
            })
        elif i % 15 == 0: # 6% de dados nulos onde não deveria
             data.append({
                'id_cliente': None,
                'nome': fake.name(),
                'email': None,
                'data_cadastro': fake.date_this_decade(),
                'valor_ultima_compra': random.uniform(10, 2000),
                'status': 'ATIVO'
            })
        else: # Dados bons
            data.append({
                'id_cliente': i,
                'nome': fake.name(),
                'email': fake.email(),
                'data_cadastro': fake.date_this_decade(),
                'valor_ultima_compra': round(random.uniform(10, 2000), 2),
                'status': random.choice(status_options)
            })
    
    df = pd.DataFrame(data)
    # Salvar como CSV para simular uma extração de "Stage"
    df.to_csv('data/stage_clientes.csv', index=False)
    print("Arquivo 'data/stage_clientes.csv' gerado com sucesso.")

if __name__ == "__main__":
    create_fake_clients(100)