import pandas as pd
import os
import sys

# --- CONFIGURAÇÃO DE CAMINHOS
# Em vez de os.getcwd(), usamos isso para pegar a pasta onde ESTE ARQUIVO está.
# Isso garante que ele ache a pasta 'data' mesmo rodando de outro lugar.
ARQUIVO_ATUAL = os.path.abspath(__file__)
PASTA_RAIZ = os.path.dirname(ARQUIVO_ATUAL)

RAW_PATH = os.path.join(PASTA_RAIZ, 'data', 'raw')
PROCESSED_PATH = os.path.join(PASTA_RAIZ, 'data', 'processed')

print(f"📍 Script rodando em: {PASTA_RAIZ}")

def run_pipeline():
    print("\n🚀 INICIANDO PIPELINE DE DADOS (ETL)...")
    
    # 1. EXTRACT
    print(f"[1/3] Lendo arquivos de: {RAW_PATH}")
    if not os.path.exists(RAW_PATH):
        print("❌ Erro: Pasta raw não encontrada.")
        return

    dfs = {}
    csv_files = [f for f in os.listdir(RAW_PATH) if f.endswith('.csv')]
    
    for file in csv_files:
        name = file.replace('olist_', '').replace('_dataset.csv', '')
        if name == 'product_category_name_translation.csv': name = 'category_translation'
        
        dfs[name] = pd.read_csv(os.path.join(RAW_PATH, file))
        # print(f"   -> Lido: {name}") # Comentei para ficar mais limpo no terminal

    # 2. TRANSFORM
    print("[2/3] Transformando e Unificando...")
    if 'orders' not in dfs or 'order_items' not in dfs:
        print("❌ Erro: Arquivos orders ou items faltando.")
        return

    df_orders = dfs['orders'].copy()
    df_items = dfs['order_items'].copy()

    # Tratamento de Datas
    cols_data = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in cols_data:
        if col in df_orders.columns:
            df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')

    # Feature Engineering
    df_orders['time_to_delivery'] = (df_orders['order_delivered_customer_date'] - df_orders['order_purchase_timestamp']).dt.days
    df_orders['is_late'] = (df_orders['order_delivered_customer_date'] > df_orders['order_estimated_delivery_date']).astype(int)

    # Merge
    fact_vendas = pd.merge(df_items, df_orders, on='order_id', how='inner')
    
    # Limpeza
    cols_drop = ['order_approved_at', 'order_delivered_carrier_date']
    fact_vendas.drop(columns=cols_drop, errors='ignore', inplace=True)

    # 3. LOAD
    print(f"[3/3] Salvando em: {PROCESSED_PATH}")
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    
    fact_vendas.to_csv(os.path.join(PROCESSED_PATH, 'fact_vendas.csv'), index=False)
    print("   ✅ fact_vendas.csv salvo!")

    dimensoes = ['products', 'customers', 'sellers', 'geolocation', 'category_translation']
    for dim in dimensoes:
        if dim in dfs:
            dfs[dim].to_csv(os.path.join(PROCESSED_PATH, f'dim_{dim}.csv'), index=False)
            print(f"   ✅ dim_{dim}.csv salvo!")

    print("\n✨ PIPELINE FINALIZADO COM SUCESSO! ✨")

if __name__ == "__main__":
    run_pipeline()