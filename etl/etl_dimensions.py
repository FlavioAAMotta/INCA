"""
ETL - CRIAÇÃO DAS TABELAS DE DIMENSÃO
======================================
Este script cria as 8 tabelas de dimensão do modelo estrela.

ATENÇÃO CRÍTICA: As funções de hash aqui definidas DEVEM ser copiadas
EXATAMENTE para o etl_fact.py. NÃO altere a lógica de hash sem reprocessar tudo!

Schema Supabase:
- dim_paciente (sexo, idade, raca_cor, nivel_instrucao, estado_civil)
- dim_localizacao (cidade_nascimento, estado_residencia, procedencia)
- dim_instituicao (codigo_atendimento, codigo_tratamento, cnes, uf, municipio)
- dim_tumor (local_detalhado, local_primario, local_propagacao, tipo_histologico, lateralidade, tnm, ptnm, estadiamento)
- dim_fatores_risco (historico_familiar, alcoolismo, tabagismo)
- dim_ocupacao (ocupacao)
- dim_tempo (ano_primeiro_diagnostico, mes, data_completa)
- dim_tratamento (data_primeiro_contato, data_inicio_tratamento, tipo_tratamento, estado_final_tratamento, razao_termino, antecedente_tratamento)

Autor: Sistema ETL RHC
Data: Outubro 2025
"""

import pandas as pd
import hashlib
from pathlib import Path
import sys


# ==============================================================================
# FUNÇÕES DE HASH - ATENÇÃO: COPIAR EXATAMENTE PARA etl_fact.py
# ==============================================================================
# 
# REGRAS CRÍTICAS:
# 1. NUNCA altere a ordem dos parâmetros
# 2. NUNCA altere como None/NULL é tratado
# 3. NUNCA altere o separador "|"
# 4. Se mudar algo aqui, DEVE mudar em etl_fact.py também
# 5. Sempre use valores BRUTOS (antes de qualquer transformação)
#
# ==============================================================================

def _normalize_for_hash(value):
    """
    Normaliza um valor para geração de hash.
    
    REGRA: 
    - None, NaN, "", "nan", "NaN", "NaT" → "NULL"
    - Qualquer outro valor → string sem espaços extras
    """
    if pd.isna(value) or value is None or str(value).strip() in ["", "nan", "NaN", "NaT", "None"]:
        return "NULL"
    return str(value).strip()


def hash_paciente(sexo, idade, raca_cor, nivel_instrucao, estado_civil):
    """
    HASH PARA dim_paciente
    
    IMPORTANTE: Esta função usa os valores BRUTOS do CSV.
    Não faça transformações antes de chamar esta função!
    """
    # Normalizar cada campo
    sexo = _normalize_for_hash(sexo)
    idade = _normalize_for_hash(idade)
    raca_cor = _normalize_for_hash(raca_cor)
    nivel_instrucao = _normalize_for_hash(nivel_instrucao)
    estado_civil = _normalize_for_hash(estado_civil)
    
    # Criar string combinada
    combined = f"{sexo}|{idade}|{raca_cor}|{nivel_instrucao}|{estado_civil}"
    
    # Gerar MD5
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_localizacao(cidade_nascimento, estado_residencia, procedencia):
    """HASH PARA dim_localizacao"""
    cidade_nascimento = _normalize_for_hash(cidade_nascimento)
    estado_residencia = _normalize_for_hash(estado_residencia)
    procedencia = _normalize_for_hash(procedencia)
    
    combined = f"{cidade_nascimento}|{estado_residencia}|{procedencia}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_instituicao(codigo_atendimento, codigo_tratamento, cnes, uf, municipio):
    """HASH PARA dim_instituicao"""
    codigo_atendimento = _normalize_for_hash(codigo_atendimento)
    codigo_tratamento = _normalize_for_hash(codigo_tratamento)
    cnes = _normalize_for_hash(cnes)
    uf = _normalize_for_hash(uf)
    municipio = _normalize_for_hash(municipio)
    
    combined = f"{codigo_atendimento}|{codigo_tratamento}|{cnes}|{uf}|{municipio}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tumor(local_detalhado, local_primario, local_propagacao, tipo_histologico, 
               lateralidade, tnm, ptnm, estadiamento):
    """HASH PARA dim_tumor"""
    local_detalhado = _normalize_for_hash(local_detalhado)
    local_primario = _normalize_for_hash(local_primario)
    local_propagacao = _normalize_for_hash(local_propagacao)
    tipo_histologico = _normalize_for_hash(tipo_histologico)
    lateralidade = _normalize_for_hash(lateralidade)
    tnm = _normalize_for_hash(tnm)
    ptnm = _normalize_for_hash(ptnm)
    estadiamento = _normalize_for_hash(estadiamento)
    
    combined = f"{local_detalhado}|{local_primario}|{local_propagacao}|{tipo_histologico}|{lateralidade}|{tnm}|{ptnm}|{estadiamento}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_fatores_risco(historico_familiar, alcoolismo, tabagismo):
    """HASH PARA dim_fatores_risco"""
    historico_familiar = _normalize_for_hash(historico_familiar)
    alcoolismo = _normalize_for_hash(alcoolismo)
    tabagismo = _normalize_for_hash(tabagismo)
    
    combined = f"{historico_familiar}|{alcoolismo}|{tabagismo}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_ocupacao(ocupacao):
    """HASH PARA dim_ocupacao"""
    ocupacao = _normalize_for_hash(ocupacao)
    combined = f"{ocupacao}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tempo(data_completa, ano, mes):
    """HASH PARA dim_tempo"""
    data_completa = _normalize_for_hash(data_completa)
    ano = _normalize_for_hash(ano)
    mes = _normalize_for_hash(mes)
    
    combined = f"{data_completa}|{ano}|{mes}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tratamento(data_primeiro_contato, data_inicio_tratamento, tipo_tratamento,
                   estado_final_tratamento, razao_termino, antecedente_tratamento):
    """HASH PARA dim_tratamento"""
    data_primeiro_contato = _normalize_for_hash(data_primeiro_contato)
    data_inicio_tratamento = _normalize_for_hash(data_inicio_tratamento)
    tipo_tratamento = _normalize_for_hash(tipo_tratamento)
    estado_final_tratamento = _normalize_for_hash(estado_final_tratamento)
    razao_termino = _normalize_for_hash(razao_termino)
    antecedente_tratamento = _normalize_for_hash(antecedente_tratamento)
    
    combined = f"{data_primeiro_contato}|{data_inicio_tratamento}|{tipo_tratamento}|{estado_final_tratamento}|{razao_termino}|{antecedente_tratamento}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


# ==============================================================================
# FUNÇÕES DE CRIAÇÃO DE DIMENSÕES
# ==============================================================================

def create_dim_paciente(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_paciente"""
    print("   [1/8] Criando dim_paciente...")
    
    # Selecionar colunas necessárias
    cols = ["sexo", "idade", "raca_cor", "instrucao", "estado_conjugal"]
    dim = df[cols].copy()
    
    # Renomear para schema do banco
    dim.columns = ["sexo", "idade", "raca_cor", "nivel_instrucao", "estado_civil"]
    
    # GERAR HASH_KEY usando valores BRUTOS
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_paciente(row['sexo'], row['idade'], row['raca_cor'], 
                     row['nivel_instrucao'], row['estado_civil'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas baseado no hash
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # AGORA aplicar transformações para o banco (após gerar hash!)
    # Converter sexo para CHAR(1): M/F
    dim['sexo'] = dim['sexo'].map(
        lambda x: 'M' if pd.notna(x) and 'MASC' in str(x).upper() 
                  else ('F' if pd.notna(x) and 'FEM' in str(x).upper() else None)
    )
    
    # Converter idade para SMALLINT
    dim['idade'] = pd.to_numeric(dim['idade'], errors='coerce')
    
    # Reordenar: hash_key primeiro
    dim = dim[['hash_key', 'sexo', 'idade', 'raca_cor', 'nivel_instrucao', 'estado_civil']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_localizacao(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_localizacao"""
    print("   [2/8] Criando dim_localizacao...")
    
    cols = ["local_nascimento", "estado_residencia", "procedencia"]
    dim = df[cols].copy()
    dim.columns = ["cidade_nascimento", "estado_residencia", "procedencia"]
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_localizacao(row['cidade_nascimento'], row['estado_residencia'], row['procedencia'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'cidade_nascimento', 'estado_residencia', 'procedencia']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_instituicao(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_instituicao"""
    print("   [3/8] Criando dim_instituicao...")
    
    cols = ["clinica_atendimento", "clinica_tratamento", "cnes", 
            "uf_unidade_hospitalar", "municipio_unidade_hospitalar"]
    dim = df[cols].copy()
    dim.columns = ["codigo_atendimento", "codigo_tratamento", "cnes", "uf", "municipio"]
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_instituicao(row['codigo_atendimento'], row['codigo_tratamento'], 
                        row['cnes'], row['uf'], row['municipio'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'codigo_atendimento', 'codigo_tratamento', 'cnes', 'uf', 'municipio']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_tumor(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_tumor"""
    print("   [4/8] Criando dim_tumor...")
    
    cols = ["localizacao_tumor_detalhada", "localizacao_tumor_primaria", 
            "localizacao_tumor_procedimento", "tipo_histologico", 
            "lateralidade", "tnm", "ptnm", "estadiamento"]
    dim = df[cols].copy()
    dim.columns = ["local_detalhado", "local_primario", "local_propagacao",
                   "tipo_histologico", "lateralidade", "tnm", "ptnm", "estadiamento"]
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_tumor(row['local_detalhado'], row['local_primario'], row['local_propagacao'],
                  row['tipo_histologico'], row['lateralidade'], row['tnm'], 
                  row['ptnm'], row['estadiamento'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'local_detalhado', 'local_primario', 'local_propagacao',
               'tipo_histologico', 'lateralidade', 'tnm', 'ptnm', 'estadiamento']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_fatores_risco(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_fatores_risco"""
    print("   [5/8] Criando dim_fatores_risco...")
    
    cols = ["historico_familiar", "alcoolismo", "tabagismo"]
    dim = df[cols].copy()
    
    # Gerar hash ANTES de converter para boolean
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_fatores_risco(row['historico_familiar'], row['alcoolismo'], row['tabagismo'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # AGORA converter para BOOLEAN (após gerar hash!)
    def to_boolean(val):
        if pd.isna(val):
            return None
        val_str = str(val).upper()
        if "SIM" in val_str or val_str == "1":
            return True
        elif "NAO" in val_str or "NÃO" in val_str or val_str == "2":
            return False
        return None
    
    dim['historico_familiar'] = dim['historico_familiar'].apply(to_boolean)
    dim['alcoolismo'] = dim['alcoolismo'].apply(to_boolean)
    dim['tabagismo'] = dim['tabagismo'].apply(to_boolean)
    
    # Reordenar
    dim = dim[['hash_key', 'historico_familiar', 'alcoolismo', 'tabagismo']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_ocupacao(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_ocupacao"""
    print("   [6/8] Criando dim_ocupacao...")
    
    dim = df[['ocupacao']].copy()
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [hash_ocupacao(row['ocupacao']) for _, row in dim.iterrows()]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'ocupacao']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_tempo"""
    print("   [7/8] Criando dim_tempo...")
    
    # Extrair datas válidas
    dates = pd.to_datetime(df['data_diagnostico'], errors='coerce')
    
    dim = pd.DataFrame({
        'data_completa': dates.dt.strftime('%Y-%m-%d'),
        'ano_primeiro_diagnostico': dates.dt.year,
        'mes': dates.dt.month
    })
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_tempo(row['data_completa'], row['ano_primeiro_diagnostico'], row['mes'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'ano_primeiro_diagnostico', 'mes', 'data_completa']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


def create_dim_tratamento(df: pd.DataFrame) -> pd.DataFrame:
    """Cria dim_tratamento"""
    print("   [8/8] Criando dim_tratamento...")
    
    cols = ["data_primeiro_contato_alt", "data_inicio_tratamento_alt",
            "primeiro_tratamento_hospital", "estado_final_tratamento",
            "razao_nao_tratamento", "diagnostico_anterior"]
    dim = df[cols].copy()
    dim.columns = ["data_primeiro_contato", "data_inicio_tratamento",
                   "tipo_tratamento", "estado_final_tratamento",
                   "razao_termino", "antecedente_tratamento"]
    
    # Gerar hash
    print("      Gerando hash_keys...")
    dim['hash_key'] = [
        hash_tratamento(row['data_primeiro_contato'], row['data_inicio_tratamento'],
                       row['tipo_tratamento'], row['estado_final_tratamento'],
                       row['razao_termino'], row['antecedente_tratamento'])
        for _, row in dim.iterrows()
    ]
    
    # Remover duplicatas
    antes = len(dim)
    dim = dim.drop_duplicates(subset=['hash_key'], keep='first').reset_index(drop=True)
    print(f"      Duplicatas removidas: {antes - len(dim):,}")
    
    # Reordenar
    dim = dim[['hash_key', 'data_primeiro_contato', 'data_inicio_tratamento',
               'tipo_tratamento', 'estado_final_tratamento', 'razao_termino', 
               'antecedente_tratamento']]
    
    print(f"      [OK] {len(dim):,} registros unicos")
    return dim


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """Processa os dados limpos e cria todas as dimensões."""
    print("=" * 70)
    print("ETL - CRIACAO DAS DIMENSOES")
    print("=" * 70)
    
    # Diretórios
    data_processed_dir = Path("data_processed")
    dimensions_dir = Path("dimensions")
    
    # Verificar se diretório processed existe
    if not data_processed_dir.exists():
        print("\nERRO: Diretorio data_processed nao encontrado!")
        print("Execute primeiro: python scripts/etl_cleaning.py")
        sys.exit(1)
    
    # Criar diretório dimensions
    dimensions_dir.mkdir(exist_ok=True)
    
    # Carregar todos os arquivos processados
    print("\nCarregando arquivos processados...")
    csv_files = sorted(data_processed_dir.glob("rhc*.csv"))
    
    if not csv_files:
        print("ERRO: Nenhum arquivo encontrado em data_processed/")
        sys.exit(1)
    
    print(f"Arquivos encontrados: {len(csv_files)}")
    
    # Consolidar todos em um único DataFrame
    dfs = []
    for csv_file in csv_files:
        print(f"   Carregando {csv_file.name}...")
        df = pd.read_csv(csv_file)
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal de registros: {len(df_all):,}")
    
    # Criar dimensões
    print("\n" + "-" * 70)
    print("CRIANDO DIMENSOES")
    print("-" * 70)
    
    dim_paciente = create_dim_paciente(df_all)
    dim_localizacao = create_dim_localizacao(df_all)
    dim_instituicao = create_dim_instituicao(df_all)
    dim_tumor = create_dim_tumor(df_all)
    dim_fatores_risco = create_dim_fatores_risco(df_all)
    dim_ocupacao = create_dim_ocupacao(df_all)
    dim_tempo = create_dim_tempo(df_all)
    dim_tratamento = create_dim_tratamento(df_all)
    
    # Salvar dimensões
    print("\n" + "-" * 70)
    print("SALVANDO DIMENSOES")
    print("-" * 70)
    
    dimensoes = {
        'dim_paciente.csv': dim_paciente,
        'dim_localizacao.csv': dim_localizacao,
        'dim_instituicao.csv': dim_instituicao,
        'dim_tumor.csv': dim_tumor,
        'dim_fatores_risco.csv': dim_fatores_risco,
        'dim_ocupacao.csv': dim_ocupacao,
        'dim_tempo.csv': dim_tempo,
        'dim_tratamento.csv': dim_tratamento
    }
    
    for filename, dim_df in dimensoes.items():
        output_path = dimensions_dir / filename
        print(f"   Salvando {filename}... ({len(dim_df):,} registros)")
        dim_df.to_csv(output_path, index=False, encoding='utf-8')
    
    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"Registros no data_processed: {len(df_all):,}")
    print(f"Dimensoes criadas: {len(dimensoes)}")
    print("\nTamanho das dimensoes:")
    for filename, dim_df in dimensoes.items():
        print(f"   {filename:30s} {len(dim_df):>10,} registros")
    print("=" * 70)
    print("\nSUCESSO: Todas as dimensoes foram criadas!")
    sys.exit(0)


if __name__ == "__main__":
    main()

