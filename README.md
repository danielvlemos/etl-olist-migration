# Case Técnico: Pipeline ETL e Migração de Dados (E-commerce)

## Visão Geral do Projeto
Este projeto consiste na construção de um pipeline de ETL (Extract, Transform, Load) para consolidar e migrar uma base fragmentada de e-commerce (dados da Olist) para um banco de dados relacional (SQLite). O objetivo foi transformar arquivos transacionais brutos em um modelo dimensional (Star Schema) pronto para análises de Business Intelligence.

## 1. O Contexto e o Legado
**A Origem:** O "legado" consistia em um ecossistema de dados fragmentado em 9 arquivos CSV separados (clientes, pedidos, itens, pagamentos, produtos, etc.), totalizando mais de 100 mil registros de vendas.

**Desafios de Qualidade:** 
* **Fragmentação Extrema:** A ausência de uma Tabela Fato consolidada exigia múltiplos cruzamentos (joins) pesados para análises simples.
* **Inconsistências Temporais:** Necessidade de conversão e padronização de múltiplos campos de data/hora que vinham como texto (strings).
* **Campos Desnecessários/Sujos:** Colunas operacionais que não agregavam valor analítico e precisavam ser expurgadas.

## 2. A Estratégia de Mapeamento (De/Para)
**Engenharia Reversa:** Realizei o mapeamento das chaves primárias e estrangeiras (`order_id`, `customer_id`, `product_id`) espalhadas pelos arquivos para entender a cardinalidade real do negócio.

**Modelagem Dimensional (Star Schema):**
* Defini a criação de uma **Tabela Fato** (`fact_vendas`) cruzando as informações de pedidos (`orders`) e itens (`order_items`).
* Isolei as informações de contorno em **Tabelas Dimensão** (`dim_products`, `dim_customers`, `dim_sellers`).
* Criei métricas de negócio (Feature Engineering) durante o mapeamento, como `time_to_delivery` (tempo de entrega em dias) e a flag `is_late` para entregas atrasadas.

## 3. A Solução de ETL e Migração
A arquitetura foi construída 100% em **Python (Pandas)** para processamento e **SQLite** para armazenamento final. O processo foi modularizado em scripts para permitir automação:
* **Extract:** O script faz a varredura automática da pasta `raw` utilizando a biblioteca `os` e carrega os arquivos em um dicionário de DataFrames.
* **Transform (`etl_pipeline.py` / `01_etl_ingestao.ipynb`):** Tratamento de datas com `pd.to_datetime`, aplicação das regras de negócio, expurgo de colunas nulas ou não utilizadas e execução do `merge` (inner join) para construir a Tabela Fato.
* **Load (`create_database.py`):** Os dados saneados são exportados primeiro para CSVs na camada `processed`. Em seguida, o script automatizado conecta-se ao SQLite e utiliza o comando `df.to_sql` para criar e popular o banco de dados final (`olist_database.db`), concluindo a migração.

## 4. Validação e Integridade
Para garantir que nenhum dado transacional fosse perdido durante o pipeline, implementei validações de contagem no próprio script:
* **Auditoria de Linhas (Shape Check):** Após o cruzamento das tabelas de itens (112.650 linhas) e pedidos, validei se a Tabela Fato final manteve a granularidade e o volume correto de registros esperados.
* **Logs de Execução:** O pipeline gera logs no terminal (`✅ fact_vendas.csv salvo!`, `✅ Tabela 'fact_vendas' inserida no banco`) validando a integridade da escrita dos dados físicos e a criação das tabelas no banco de dados.

---
*Projeto desenvolvido por Daniel Lemos.*
