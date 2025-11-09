"""
ETL - LIMPEZA E PADRONIZAÇÃO DOS DADOS
=======================================
Este script processa os arquivos CSV brutos do RHC (Registro Hospitalar de Câncer)
e gera arquivos limpos e padronizados para uso nas etapas seguintes do ETL.

IMPORTANTE: Este script NÃO gera hashes. Apenas limpa e padroniza os dados.

Autor: Sistema ETL RHC
Data: Outubro 2025
"""

import pandas as pd
from pathlib import Path
import sys

# ==============================================================================
# MAPEAMENTO DE COLUNAS (CSV RAW → NOMES PADRONIZADOS)
# ==============================================================================

COLUMN_MAP = {
    # Identificação
    "TPCASO": "tipo_caso",
    
    # Paciente
    "SEXO": "sexo",
    "IDADE": "idade",
    "RACACOR": "raca_cor",
    "INSTRUC": "instrucao",
    "ESTCONJ": "estado_conjugal",
    
    # Localização
    "LOCALNAS": "local_nascimento",
    "ESTADRES": "estado_residencia",
    "PROCEDEN": "procedencia",
    
    # Instituição
    "CLIATEN": "clinica_atendimento",
    "CLITRAT": "clinica_tratamento",
    "CNES": "cnes",
    "UFUH": "uf_unidade_hospitalar",
    "MUUH": "municipio_unidade_hospitalar",
    
    # Tumor
    "LOCTUDET": "localizacao_tumor_detalhada",
    "LOCTUPRI": "localizacao_tumor_primaria",
    "LOCTUPRO": "localizacao_tumor_procedimento",
    "TIPOHIST": "tipo_histologico",
    "LATERALI": "lateralidade",
    "TNM": "tnm",
    "PTNM": "ptnm",
    "ESTADIAM": "estadiamento",
    
    # Fatores de Risco
    "HISTFAMC": "historico_familiar",
    "ALCOOLIS": "alcoolismo",
    "TABAGISM": "tabagismo",
    
    # Ocupação
    "OCUPACAO": "ocupacao",
    
    # Tempo
    "ANOPRIDI": "ano_primeiro_diagnostico",
    "DTDIAGNO": "data_diagnostico",
    
    # Tratamento
    "DTPRICON": "data_primeiro_contato",
    "DTINITRT": "data_inicio_tratamento",
    "DATAPRICON": "data_primeiro_contato_alt",
    "DATAINITRT": "data_inicio_tratamento_alt",
    "PRITRATH": "primeiro_tratamento_hospital",
    "ESTDFIMT": "estado_final_tratamento",
    "RZNTR": "razao_nao_tratamento",
    "DIAGANT": "diagnostico_anterior",
    "ANTRI": "antecedente_tratamento",
    
    # Outros
    "DATAOBITO": "data_obito",
    "VALOR_TOT": "valor_total",
    "MAISUMTU": "mais_um_tumor",
    "ORIENC": "origem_encaminhamento",
    "EXDIAG": "exame_diagnostico",
    "BASMAIMP": "base_mais_importante",
    "BASDIAGSP": "base_diagnostico_sp",
    "OUTROESTA": "outro_estadiamento",
    "DTTRIAGE": "data_triagem"
}


# ==============================================================================
# FUNÇÕES DE LIMPEZA
# ==============================================================================

def substituir_valores_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui valores inválidos por None.
    
    Valores considerados inválidos:
    - " / / "
    - "  /  /"
    - "."
    - "Sem informacao"
    - "Sem Informacao"
    - "SEM INFORMACAO"
    - Strings vazias
    """
    valores_invalidos = [
        " / / ",
        "  /  /",
        ".",
        "Sem informacao",
        "Sem Informacao",
        "SEM INFORMACAO",
        ""
    ]
    
    # Substituir em todo o DataFrame
    for valor in valores_invalidos:
        df = df.replace(valor, None)
    
    return df


def padronizar_datas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza colunas de data de DD/MM/YYYY para YYYY-MM-DD.
    """
    colunas_data = [
        "data_diagnostico",
        "data_obito",
        "data_primeiro_contato",
        "data_inicio_tratamento",
        "data_primeiro_contato_alt",
        "data_inicio_tratamento_alt",
        "data_triagem"
    ]
    
    for col in colunas_data:
        if col in df.columns:
            try:
                # Converter para datetime
                df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
                # Converter para string no formato YYYY-MM-DD
                df[col] = df[col].dt.strftime("%Y-%m-%d")
                # Substituir NaT por None
                df[col] = df[col].replace("NaT", None)
            except Exception as e:
                print(f"      AVISO: Erro ao processar coluna {col}: {e}")
    
    return df


def padronizar_categoricas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza valores de colunas categóricas.
    - Remove espaços extras
    - Mantém capitalização original (não converte tudo para maiúscula)
    """
    colunas_string = df.select_dtypes(include=['object']).columns
    
    for col in colunas_string:
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    
    return df


def remover_linhas_vazias(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas completamente vazias."""
    antes = len(df)
    df = df.dropna(how='all')
    removidas = antes - len(df)
    
    if removidas > 0:
        print(f"      Linhas vazias removidas: {removidas}")
    
    return df


def remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas duplicadas."""
    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    
    if removidas > 0:
        print(f"      Duplicatas removidas: {removidas}")
    
    return df


# ==============================================================================
# FUNÇÃO PRINCIPAL DE LIMPEZA
# ==============================================================================

def limpar_arquivo(arquivo_path: Path, output_dir: Path) -> bool:
    """
    Limpa e padroniza um único arquivo CSV.
    
    Args:
        arquivo_path: Caminho do arquivo bruto
        output_dir: Diretório de saída
        
    Returns:
        True se processado com sucesso, False caso contrário
    """
    try:
        print(f"\n   Processando: {arquivo_path.name}")
        
        # 1. Ler arquivo
        print("      Lendo arquivo...")
        df = pd.read_csv(
            arquivo_path,
            encoding='latin1',
            low_memory=False
        )
        print(f"      Registros originais: {len(df):,}")
        
        # 2. Renomear colunas
        print("      Renomeando colunas...")
        df = df.rename(columns=COLUMN_MAP)
        
        # 3. Substituir valores inválidos
        print("      Substituindo valores invalidos por None...")
        df = substituir_valores_invalidos(df)
        
        # 4. Padronizar datas
        print("      Padronizando datas...")
        df = padronizar_datas(df)
        
        # 5. Padronizar categóricas
        print("      Padronizando categoricas...")
        df = padronizar_categoricas(df)
        
        # 6. Remover linhas vazias
        print("      Removendo linhas vazias...")
        df = remover_linhas_vazias(df)
        
        # 7. Remover duplicatas
        print("      Removendo duplicatas...")
        df = remover_duplicatas(df)
        
        # 8. Salvar arquivo limpo
        output_file = output_dir / arquivo_path.name
        print(f"      Salvando em: {output_file}")
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"      CONCLUIDO: {len(df):,} registros salvos")
        return True
        
    except Exception as e:
        print(f"      ERRO ao processar {arquivo_path.name}: {e}")
        return False


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """Processa todos os arquivos CSV brutos."""
    print("=" * 70)
    print("ETL - LIMPEZA E PADRONIZACAO DOS DADOS")
    print("=" * 70)
    
    # Diretórios
    data_raw_dir = Path("data_raw")
    data_processed_dir = Path("data_processed")
    
    # Verificar se diretório raw existe
    if not data_raw_dir.exists():
        print("\nERRO: Diretorio data_raw nao encontrado!")
        sys.exit(1)
    
    # Criar diretório processed se não existir
    data_processed_dir.mkdir(exist_ok=True)
    
    # Listar arquivos CSV
    csv_files = sorted(data_raw_dir.glob("rhc*.csv"))
    
    if not csv_files:
        print("\nERRO: Nenhum arquivo rhc*.csv encontrado em data_raw/")
        sys.exit(1)
    
    print(f"\nArquivos encontrados: {len(csv_files)}")
    
    # Processar cada arquivo
    sucessos = 0
    erros = 0
    
    for csv_file in csv_files:
        if limpar_arquivo(csv_file, data_processed_dir):
            sucessos += 1
        else:
            erros += 1
    
    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO DO PROCESSAMENTO")
    print("=" * 70)
    print(f"Arquivos processados com sucesso: {sucessos}")
    print(f"Arquivos com erro: {erros}")
    print("=" * 70)
    
    if erros > 0:
        print("\nAVISO: Alguns arquivos apresentaram erros!")
        sys.exit(1)
    else:
        print("\nSUCESSO: Todos os arquivos foram processados!")
        sys.exit(0)


if __name__ == "__main__":
    main()

