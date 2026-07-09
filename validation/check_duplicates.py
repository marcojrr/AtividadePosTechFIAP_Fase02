import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'alfabetizacao-datalake'

# Funções utilitárias

def le_parquet_s3(caminho_s3):
    obj = s3.get_object(Bucket=BUCKET, Key=caminho_s3)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def verifica_duplicatas(df, nome, chave_primaria):
    # Verifica duplicatas totalmente iguais e de chave primária
    print(f"\n--- {nome} ---")

    # Duplicatas totais que são linhas completamente iguais
    dup_totais = df.duplicated().sum()
    if dup_totais > 0:
        print(f"  ⚠️  Duplicatas totais: {dup_totais} linhas")
    else:
        print(f"  ✅ Sem duplicatas totais")

    # Duplicatas de chave, mesma chave com valores diferentes
    dup_chave = df.duplicated(subset=chave_primaria).sum()
    if dup_chave > 0:
        print(f"  ⚠️  Duplicatas de chave {chave_primaria}: {dup_chave} registros")
        print(f"  Exemplos:")
        exemplos = df[df.duplicated(subset=chave_primaria, keep=False)].sort_values(chave_primaria).head(6)
        print(exemplos[chave_primaria].to_string(index=False))
    else:
        print(f"  ✅ Sem duplicatas de chave {chave_primaria}")

# Leitura da Silver

print("=== Validação de Duplicatas — Camada Silver ===")

anos = [2023, 2024]

alunos = pd.concat([le_parquet_s3(f"silver/alunos/year={ano}/dados.parquet") for ano in anos])
uf = pd.concat([le_parquet_s3(f"silver/uf/year={ano}/dados.parquet") for ano in anos])
municipio = pd.concat([le_parquet_s3(f"silver/municipio/year={ano}/dados.parquet") for ano in anos])
meta_brasil = pd.concat([le_parquet_s3(f"silver/meta_alfabetizacao_brasil/year={ano}/dados.parquet") for ano in anos])
meta_uf = pd.concat([le_parquet_s3(f"silver/meta_alfabetizacao_uf/year={ano}/dados.parquet") for ano in anos])
meta_municipio = pd.concat([le_parquet_s3(f"silver/meta_alfabetizacao_municipio/year={ano}/dados.parquet") for ano in anos])

# Validações

verifica_duplicatas(
    alunos,
    nome='Alunos',
    chave_primaria=['id_aluno', 'ano']
)

verifica_duplicatas(
    uf,
    nome='UF',
    chave_primaria=['sigla_uf', 'rede', 'ano']
)

verifica_duplicatas(
    municipio,
    nome='Município',
    chave_primaria=['id_municipio', 'rede', 'ano']
)

verifica_duplicatas(
    meta_brasil,
    nome='Meta Brasil',
    chave_primaria=['rede', 'ano']
)

verifica_duplicatas(
    meta_uf,
    nome='Meta UF',
    chave_primaria=['sigla_uf', 'rede', 'ano']
)

verifica_duplicatas(
    meta_municipio,
    nome='Meta Município',
    chave_primaria=['id_municipio', 'rede', 'ano']
)

print("\n=== Validação concluída ===")
