"""
ETL - CRIAÇÃO DA TABELA FATO
=============================
Este script cria a tabela fato fato_casos_oncologicos.

⚠️⚠️⚠️ ATENÇÃO CRÍTICA SOBRE HASHES ⚠️⚠️⚠️

As funções de hash abaixo foram COPIADAS EXATAMENTE de etl_dimensions.py.
NÃO MODIFIQUE NADA AQUI SEM MODIFICAR LÁ TAMBÉM!

Se os hashes não baterem, os JOINs retornarão 0 registros!

Autor: Sistema ETL RHC
Data: Outubro 2025
"""

import pandas as pd
import hashlib
from pathlib import Path
import json
from datetime import datetime
import sys


# ==============================================================================
# FUNÇÕES DE HASH - COPIADAS DE etl_dimensions.py - NÃO MODIFICAR!
# ==============================================================================
# 
# ⚠️⚠️⚠️ AVISO CRÍTICO ⚠️⚠️⚠️
# 
# Estas funções foram copiadas EXATAMENTE de etl_dimensions.py
# 
# SE VOCÊ MODIFICAR ALGO AQUI:
# 1. Os hashes serão diferentes
# 2. Os JOINs não funcionarão
# 3. O dashboard ficará vazio
# 4. Será necessário reprocessar TUDO
# 
# SE PRECISAR MUDAR:
# 1. Modifique em etl_dimensions.py
# 2. Copie e cole aqui
# 3. Reprocesse dimensões E fato
# 4. Recarregue no Supabase
# 
# ==============================================================================

def _normalize_for_hash(value):
    """
    Normaliza um valor para geração de hash.
    
    REGRA: 
    - None, NaN, "", "nan", "NaN", "NaT" → "NULL"
    - Qualquer outro valor → string sem espaços extras
    
    ⚠️ NÃO MODIFICAR - COPIADO DE etl_dimensions.py
    """
    if pd.isna(value) or value is None or str(value).strip() in ["", "nan", "NaN", "NaT", "None"]:
        return "NULL"
    return str(value).strip()


def hash_paciente(sexo, idade, raca_cor, nivel_instrucao, estado_civil):
    """
    HASH PARA dim_paciente
    ⚠️ NÃO MODIFICAR - COPIADO DE etl_dimensions.py
    """
    sexo = _normalize_for_hash(sexo)
    idade = _normalize_for_hash(idade)
    raca_cor = _normalize_for_hash(raca_cor)
    nivel_instrucao = _normalize_for_hash(nivel_instrucao)
    estado_civil = _normalize_for_hash(estado_civil)
    
    combined = f"{sexo}|{idade}|{raca_cor}|{nivel_instrucao}|{estado_civil}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_localizacao(cidade_nascimento, estado_residencia, procedencia):
    """HASH PARA dim_localizacao - ⚠️ NÃO MODIFICAR"""
    cidade_nascimento = _normalize_for_hash(cidade_nascimento)
    estado_residencia = _normalize_for_hash(estado_residencia)
    procedencia = _normalize_for_hash(procedencia)
    
    combined = f"{cidade_nascimento}|{estado_residencia}|{procedencia}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_instituicao(codigo_atendimento, codigo_tratamento, cnes, uf, municipio):
    """HASH PARA dim_instituicao - ⚠️ NÃO MODIFICAR"""
    codigo_atendimento = _normalize_for_hash(codigo_atendimento)
    codigo_tratamento = _normalize_for_hash(codigo_tratamento)
    cnes = _normalize_for_hash(cnes)
    uf = _normalize_for_hash(uf)
    municipio = _normalize_for_hash(municipio)
    
    combined = f"{codigo_atendimento}|{codigo_tratamento}|{cnes}|{uf}|{municipio}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tumor(local_detalhado, local_primario, local_propagacao, tipo_histologico, 
               lateralidade, tnm, ptnm, estadiamento):
    """HASH PARA dim_tumor - ⚠️ NÃO MODIFICAR"""
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
    """HASH PARA dim_fatores_risco - ⚠️ NÃO MODIFICAR"""
    historico_familiar = _normalize_for_hash(historico_familiar)
    alcoolismo = _normalize_for_hash(alcoolismo)
    tabagismo = _normalize_for_hash(tabagismo)
    
    combined = f"{historico_familiar}|{alcoolismo}|{tabagismo}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_ocupacao(ocupacao):
    """HASH PARA dim_ocupacao - ⚠️ NÃO MODIFICAR"""
    ocupacao = _normalize_for_hash(ocupacao)
    combined = f"{ocupacao}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tempo(data_completa, ano, mes):
    """HASH PARA dim_tempo - ⚠️ NÃO MODIFICAR"""
    data_completa = _normalize_for_hash(data_completa)
    ano = _normalize_for_hash(ano)
    mes = _normalize_for_hash(mes)
    
    combined = f"{data_completa}|{ano}|{mes}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def hash_tratamento(data_primeiro_contato, data_inicio_tratamento, tipo_tratamento,
                   estado_final_tratamento, razao_termino, antecedente_tratamento):
    """HASH PARA dim_tratamento - ⚠️ NÃO MODIFICAR"""
    data_primeiro_contato = _normalize_for_hash(data_primeiro_contato)
    data_inicio_tratamento = _normalize_for_hash(data_inicio_tratamento)
    tipo_tratamento = _normalize_for_hash(tipo_tratamento)
    estado_final_tratamento = _normalize_for_hash(estado_final_tratamento)
    razao_termino = _normalize_for_hash(razao_termino)
    antecedente_tratamento = _normalize_for_hash(antecedente_tratamento)
    
    combined = f"{data_primeiro_contato}|{data_inicio_tratamento}|{tipo_tratamento}|{estado_final_tratamento}|{razao_termino}|{antecedente_tratamento}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


# ==============================================================================
# FUNÇÕES DE CHECKPOINT
# ==============================================================================

def load_checkpoint(checkpoint_file: Path) -> dict:
    """Carrega checkpoint do processamento."""
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return {"processed_files": [], "last_update": None}


def save_checkpoint(checkpoint_file: Path, checkpoint_data: dict):
    """Salva checkpoint do processamento."""
    checkpoint_data["last_update"] = datetime.now().isoformat()
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)


# ==============================================================================
# PROCESSAMENTO DE BATCH
# ==============================================================================

def process_batch(df_batch: pd.DataFrame, batch_name: str) -> pd.DataFrame:
    """
    Processa um batch de dados e gera a tabela fato.
    
    IMPORTANTE: Usa as MESMAS funções de hash de etl_dimensions.py!
    """
    print(f"\n   Processando: {batch_name} ({len(df_batch):,} registros)")
    
    # ========================================================================
    # GERAR HASH KEYS PARA FOREIGN KEYS
    # ========================================================================
    # ATENÇÃO: Usando EXATAMENTE as mesmas funções de etl_dimensions.py!
    # ========================================================================
    
    print("      Gerando hash_paciente...")
    df_batch['paciente_id'] = [
        hash_paciente(row['sexo'], row['idade'], row['raca_cor'], 
                     row['instrucao'], row['estado_conjugal'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_localizacao...")
    df_batch['localizacao_id'] = [
        hash_localizacao(row['local_nascimento'], row['estado_residencia'], row['procedencia'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_instituicao...")
    df_batch['instituicao_id'] = [
        hash_instituicao(row['clinica_atendimento'], row['clinica_tratamento'],
                        row['cnes'], row['uf_unidade_hospitalar'], row['municipio_unidade_hospitalar'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_tumor...")
    df_batch['tumor_id'] = [
        hash_tumor(row['localizacao_tumor_detalhada'], row['localizacao_tumor_primaria'],
                  row['localizacao_tumor_procedimento'], row['tipo_histologico'],
                  row['lateralidade'], row['tnm'], row['ptnm'], row['estadiamento'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_fatores_risco...")
    df_batch['fatores_id'] = [
        hash_fatores_risco(row['historico_familiar'], row['alcoolismo'], row['tabagismo'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_ocupacao...")
    df_batch['ocupacao_id'] = [
        hash_ocupacao(row['ocupacao'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_tempo...")
    # Para tempo, extrair ano e mês da data_diagnostico
    # IMPORTANTE: Usar EXATAMENTE a mesma lógica de etl_dimensions.py!
    df_batch['data_diag_dt'] = pd.to_datetime(df_batch['data_diagnostico'], errors='coerce')
    df_batch['data_completa_str'] = df_batch['data_diag_dt'].dt.strftime('%Y-%m-%d')
    df_batch['ano_val'] = df_batch['data_diag_dt'].dt.year
    df_batch['mes_val'] = df_batch['data_diag_dt'].dt.month
    
    df_batch['tempo_id'] = [
        hash_tempo(row['data_completa_str'], row['ano_val'], row['mes_val'])
        for _, row in df_batch.iterrows()
    ]
    
    print("      Gerando hash_tratamento...")
    df_batch['tratamento_id'] = [
        hash_tratamento(row['data_primeiro_contato_alt'], row['data_inicio_tratamento_alt'],
                       row['primeiro_tratamento_hospital'], row['estado_final_tratamento'],
                       row['razao_nao_tratamento'], row['diagnostico_anterior'])
        for _, row in df_batch.iterrows()
    ]
    
    # ========================================================================
    # MONTAR TABELA FATO
    # ========================================================================
    
    print("      Montando tabela fato...")
    fact_batch = pd.DataFrame({
        # Foreign keys (hashes)
        'paciente_id': df_batch['paciente_id'],
        'localizacao_id': df_batch['localizacao_id'],
        'instituicao_id': df_batch['instituicao_id'],
        'tumor_id': df_batch['tumor_id'],
        'fatores_id': df_batch['fatores_id'],
        'tempo_id': df_batch['tempo_id'],
        'tratamento_id': df_batch['tratamento_id'],
        'ocupacao_id': df_batch['ocupacao_id'],
        
        # Métricas e atributos da fato
        'case_code': df_batch['tipo_caso'],
        'data_diagnostico': df_batch['data_diagnostico'],
        'data_obito': df_batch['data_obito'],
        'valor_total': pd.to_numeric(df_batch['valor_total'], errors='coerce'),
        'multiplos_tumores': df_batch['mais_um_tumor'].map(
            lambda x: True if str(x) == '1' else (False if str(x) == '0' else None)
        ),
        'orientacao': df_batch['origem_encaminhamento'],
        'exame_diagnostico': df_batch['exame_diagnostico'],
        'diagnostico_anterior': df_batch['diagnostico_anterior'],
        'base_diagnostico': df_batch['base_mais_importante'],
        'base_diagnostico_suplementar': df_batch['base_diagnostico_sp'],
        'outro_estadio': df_batch['outro_estadiamento'],
        'sobreviveu': df_batch['data_obito'].isna()
    })
    
    print(f"      [OK] Batch processado: {len(fact_batch):,} registros")
    return fact_batch


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """Processa os dados limpos e cria a tabela fato."""
    print("=" * 70)
    print("ETL - CRIACAO DA TABELA FATO")
    print("=" * 70)
    
    # Diretórios
    data_processed_dir = Path("data_processed")
    dimensions_dir = Path("dimensions")
    checkpoint_file = dimensions_dir / ".checkpoint_fact.json"
    
    # Verificar se dimensões existem
    if not dimensions_dir.exists():
        print("\nERRO: Diretorio dimensions nao encontrado!")
        print("Execute primeiro: python scripts/etl_dimensions.py")
        sys.exit(1)
    
    # Verificar se data_processed existe
    if not data_processed_dir.exists():
        print("\nERRO: Diretorio data_processed nao encontrado!")
        print("Execute primeiro: python scripts/etl_cleaning.py")
        sys.exit(1)
    
    # Listar arquivos processados
    csv_files = sorted(data_processed_dir.glob("rhc*.csv"))
    
    if not csv_files:
        print("ERRO: Nenhum arquivo encontrado em data_processed/")
        sys.exit(1)
    
    print(f"\nArquivos encontrados: {len(csv_files)}")
    
    # Carregar checkpoint
    checkpoint = load_checkpoint(checkpoint_file)
    processed_files = set(checkpoint.get("processed_files", []))
    
    # Arquivos pendentes
    pending_files = [f for f in csv_files if f.name not in processed_files]
    
    if not pending_files:
        print("\nTodos os arquivos ja foram processados!")
        print("Para reprocessar, delete o arquivo: dimensions/.checkpoint_fact.json")
        
        # Consolidar batches se existirem
        batch_files = sorted(dimensions_dir.glob("fato_batch_*.csv"))
        if batch_files:
            print(f"\nConsolidando {len(batch_files)} batches...")
            dfs = [pd.read_csv(f) for f in batch_files]
            df_final = pd.concat(dfs, ignore_index=True)
            
            output_file = dimensions_dir / "fato_casos_oncologicos.csv"
            print(f"Salvando tabela fato final: {output_file}")
            df_final.to_csv(output_file, index=False, encoding='utf-8')
            
            # Remover batches
            for batch_file in batch_files:
                batch_file.unlink()
            
            print(f"\n[OK] Tabela fato final: {len(df_final):,} registros")
        
        sys.exit(0)
    
    print(f"Arquivos pendentes: {len(pending_files)}")
    print(f"Ja processados: {len(processed_files)}")
    
    # Processar arquivos pendentes
    print("\n" + "-" * 70)
    print("PROCESSANDO BATCHES")
    print("-" * 70)
    
    for i, csv_file in enumerate(pending_files, 1):
        print(f"\n[{i}/{len(pending_files)}] {csv_file.name}")
        
        # Carregar arquivo
        df_batch = pd.read_csv(csv_file)
        
        # Processar batch
        fact_batch = process_batch(df_batch, csv_file.name)
        
        # Salvar batch
        batch_output = dimensions_dir / f"fato_batch_{csv_file.stem}.csv"
        print(f"      Salvando batch: {batch_output.name}")
        fact_batch.to_csv(batch_output, index=False, encoding='utf-8')
        
        # Atualizar checkpoint
        processed_files.add(csv_file.name)
        checkpoint["processed_files"] = list(processed_files)
        save_checkpoint(checkpoint_file, checkpoint)
        
        print(f"      [OK] Checkpoint atualizado")
    
    # Consolidar todos os batches
    print("\n" + "-" * 70)
    print("CONSOLIDANDO BATCHES")
    print("-" * 70)
    
    batch_files = sorted(dimensions_dir.glob("fato_batch_*.csv"))
    print(f"Batches encontrados: {len(batch_files)}")
    
    dfs = []
    for batch_file in batch_files:
        print(f"   Carregando {batch_file.name}...")
        df = pd.read_csv(batch_file)
        dfs.append(df)
    
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Salvar tabela fato final
    output_file = dimensions_dir / "fato_casos_oncologicos.csv"
    print(f"\nSalvando tabela fato final: {output_file}")
    df_final.to_csv(output_file, index=False, encoding='utf-8')
    
    # Remover batches temporários
    print("\nRemovendo batches temporarios...")
    for batch_file in batch_files:
        batch_file.unlink()
    
    # Remover checkpoint
    if checkpoint_file.exists():
        checkpoint_file.unlink()
    
    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"Total de registros na fato: {len(df_final):,}")
    print(f"Arquivo gerado: dimensions/fato_casos_oncologicos.csv")
    print("=" * 70)
    print("\nSUCESSO: Tabela fato criada!")
    print("\nProximo passo: python scripts/validate_integrity.py")
    sys.exit(0)


if __name__ == "__main__":
    main()

