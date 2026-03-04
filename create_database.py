import sqlite3
import pandas as pd
import os

# Caminhos
PASTA_PROCESSED = os.path.join(os.getcwd(), 'data', 'processed')
DB_PATH = os.path.join(os.getcwd(), 'data', 'olist_database.db')

def create_sql_storage():
    print("🗄️ Iniciando criação do Banco de Dados...")
    
    # 1. Conecta ao SQLite (se não existir, ele cria o arquivo)
    conn = sqlite3.connect(DB_PATH)
    
    # 2. Lista os arquivos processados
    files = [f for f in os.listdir(PASTA_PROCESSED) if f.endswith('.csv')]
    
    for file in files:
        table_name = file.replace('.csv', '')
        file_path = os.path.join(PASTA_PROCESSED, file)
        
        # Lê o CSV tratado
        df = pd.read_csv(file_path)
        
        # Grava no Banco de Dados
        # if_exists='replace' garante que se você rodar de novo, ele atualiza os dados
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"   ✅ Tabela '{table_name}' inserida no banco.")

    conn.close()
    print(f"\n✨ Banco de Dados gerado com sucesso em: {DB_PATH}")

if __name__ == "__main__":
    create_sql_storage()