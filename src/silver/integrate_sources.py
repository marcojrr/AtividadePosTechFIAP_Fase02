import boto3
import pandas as pd
from io import BytesIO
import os

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'alfabetizacao-datalake'

# Funções utilitárias

def le_parquet_s3(caminho_s3):
    obj = s3.get_object(Bucket=BUCKET, Key=caminho_s3)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def salva_parquet_s3(df, caminho_s3):
    caminho_local = f"temp_{caminho_s3.replace('/', '_')}"
    df.to_parquet(caminho_local, index=False)
    s3.upload_file(caminho_local, BUCKET, caminho_s3)
    os.remove(caminho_local)
    print(f"✓ Salvo: {caminho_s3}")

# Leitura da Silver

print("Lendo bases da Silver\n")

anos = [2023, 2024]

alunos = pd.concat([le_parquet_s3(f"silver/alunos/year={ano}/dados.parquet") for ano in anos])
municipio = pd.concat([le_parquet_s3(f"silver/municipio/year={ano}/dados.parquet") for ano in anos])

# Padroniza colunas de alunos para minúsculo
alunos.columns = alunos.columns.str.lower()

# Join de Alunos + Município
# Adiciona indicadores agregados do município a cada aluno

print("Integrando alunos com município\n")

municipio_renamed = municipio[[
    'ano', 'id_municipio', 'rede',
    'taxa_alfabetizacao', 'media_portugues'
]].rename(columns={
    'taxa_alfabetizacao': 'mun_taxa_alfabetizacao',
    'media_portugues':    'mun_media_portugues'
})

df_integrado = pd.merge(
    alunos,
    municipio_renamed,
    on=['ano', 'id_municipio', 'rede'],
    how='left'
)

# Verifica se houve multiplicação de linhas
assert df_integrado.shape[0] == alunos.shape[0], \
    f"ERRO: join multiplicou linhas! Esperado {alunos.shape[0]}, obtido {df_integrado.shape[0]}"

print(f"  Registros após join: {df_integrado.shape[0]}")
print(f"  Colunas finais: {df_integrado.shape[1]}")
print(f"\n{df_integrado.columns.tolist()}")

# Salva tabela integrada na Silver

print("\nSalvando tabela integrada na Silver\n")

for ano in anos:
    df_ano = df_integrado[df_integrado['ano'] == ano]
    salva_parquet_s3(df_ano, f"silver/integrado/year={ano}/dados.parquet")

print("\n✓ Integração Silver concluída!")