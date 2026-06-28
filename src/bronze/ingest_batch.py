# Importa as bibliotecas necessárias
import pandas as pd
import os
import boto3

# Carrega os dados das bases CSV
uf = pd.read_csv('data/raw/uf.csv')
municipio = pd.read_csv('data/raw/municipio.csv')
meta_alf_uf = pd.read_csv('data/raw/meta_alfabetizacao_uf.csv')
meta_alf_municipio = pd.read_csv('data/raw/meta_alfabetizacao_municipio.csv')
meta_alf_brasil = pd.read_csv('data/raw/meta_alfabetizacao_brasil.csv')
alunos = pd.read_csv('data/raw/alunos.csv')

# Configurações do S3
s3 = boto3.client('s3', region_name='us-east-1')
bucket = 'alfabetizacao-datalake'

# Define a função para processar cada base de dados
def processa_base(df, col_ano, nome_df):
    for ano in [2023, 2024]:
        df_ano = df[df[col_ano] == ano]
        
        caminho_local = f"temp_{nome_df}_{ano}.parquet"
        caminho_s3 = f"bronze/{nome_df}/year={ano}/dados.parquet"
        
        # 1. Salva localmente
        df_ano.to_parquet(caminho_local)
        
        # 2. Faz upload pro S3
        s3.upload_file(
            caminho_local,   
            bucket,  
            caminho_s3    
        )
        
        # 3. Deleta o arquivo temporário local
        os.remove(caminho_local)
        
        print(f"✓ {nome_df} year={ano} enviado para o S3")

# Executa o processamento de cada base
processa_base(uf, 'ano', 'uf')
processa_base(municipio, 'ano', 'municipio')
processa_base(meta_alf_uf, 'ano', 'meta_alfabetizacao_uf')
processa_base(meta_alf_municipio, 'ano', 'meta_alfabetizacao_municipio')
processa_base(meta_alf_brasil, 'ano', 'meta_alfabetizacao_brasil')
processa_base(alunos, 'NU_ANO_AVALIACAO', 'alunos')
