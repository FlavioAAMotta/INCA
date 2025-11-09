"""
VALIDAÇÃO DE INTEGRIDADE DOS HASHES
====================================
Este script verifica se os hash_keys gerados na tabela fato correspondem
aos hash_keys das dimensões, garantindo que os JOINs funcionarão corretamente.

O que este script faz:
1. Carrega a tabela fato
2. Carrega cada dimensão
3. Verifica se os hashes da fato existem nas dimensões
4. Reporta a taxa de correspondência (deve ser 100%)
5. Identifica problemas se houver

Autor: Sistema ETL RHC
Data: Outubro 2025
"""

import pandas as pd
from pathlib import Path
import sys


# ==============================================================================
# FUNÇÕES DE VALIDAÇÃO
# ==============================================================================

def validate_dimension(df_fact: pd.DataFrame, df_dim: pd.DataFrame, 
                      fact_col: str, dim_name: str, sample_size: int = 10000) -> tuple:
    """
    Valida se os hashes da fato existem na dimensão.
    
    Args:
        df_fact: DataFrame da tabela fato
        df_dim: DataFrame da dimensão
        fact_col: Nome da coluna na fato (ex: 'paciente_id')
        dim_name: Nome da dimensão (para mensagens)
        sample_size: Quantidade de registros para testar
        
    Returns:
        tuple: (sucesso: bool, taxa_match: float, total_fato: int, total_dim: int, mensagem: str)
    """
    # Obter hashes únicos
    hashes_fato = set(df_fact[fact_col].dropna().unique())
    hashes_dim = set(df_dim['hash_key'].dropna().unique())
    
    # Calcular correspondência
    matches = len(hashes_fato & hashes_dim)  # Interseção
    total_fato = len(hashes_fato)
    total_dim = len(hashes_dim)
    
    if total_fato == 0:
        return False, 0.0, 0, total_dim, f"{dim_name}: Nenhum hash na fato para validar!"
    
    taxa = (matches / total_fato) * 100
    
    # Determinar sucesso
    sucesso = taxa == 100.0
    
    # Mensagem
    if sucesso:
        msg = f"{dim_name}: OK - 100% dos hashes batem ({matches:,}/{total_fato:,})"
    else:
        nao_encontrados = total_fato - matches
        msg = f"{dim_name}: ERRO - Apenas {taxa:.1f}% batem ({matches:,}/{total_fato:,}) - {nao_encontrados:,} hashes nao encontrados!"
    
    return sucesso, taxa, total_fato, total_dim, msg


def print_hash_examples(df_fact: pd.DataFrame, df_dim: pd.DataFrame, 
                       fact_col: str, dim_name: str, num_examples: int = 5):
    """Imprime exemplos de hashes que não batem."""
    hashes_fato = set(df_fact[fact_col].dropna().unique())
    hashes_dim = set(df_dim['hash_key'].dropna().unique())
    
    # Hashes que estão na fato mas não na dimensão
    nao_encontrados = hashes_fato - hashes_dim
    
    if nao_encontrados:
        print(f"\n      Exemplos de hashes na fato NAO encontrados em {dim_name}:")
        for i, hash_val in enumerate(list(nao_encontrados)[:num_examples], 1):
            print(f"         {i}. {hash_val}")
        
        if len(nao_encontrados) > num_examples:
            print(f"         ... e mais {len(nao_encontrados) - num_examples:,} hashes")
    
    # Hashes que estão na dimensão mas não na fato (isso é normal)
    nao_usados = hashes_dim - hashes_fato
    if nao_usados:
        print(f"\n      INFO: {len(nao_usados):,} hashes em {dim_name} nao sao usados na fato (normal)")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """Valida a integridade dos hashes entre dimensões e fato."""
    print("=" * 70)
    print("VALIDACAO DE INTEGRIDADE DOS HASHES")
    print("=" * 70)
    
    dimensions_dir = Path("dimensions")
    fact_file = dimensions_dir / "fato_casos_oncologicos.csv"
    
    # Verificar se tabela fato existe
    if not fact_file.exists():
        print("\nERRO: Arquivo fato_casos_oncologicos.csv nao encontrado!")
        print("Execute primeiro: python scripts/etl_fact.py")
        sys.exit(1)
    
    # Carregar tabela fato
    print("\n1. Carregando tabela fato...")
    try:
        df_fact = pd.read_csv(fact_file)
        print(f"   Registros na fato: {len(df_fact):,}")
    except Exception as e:
        print(f"   ERRO ao carregar fato: {e}")
        sys.exit(1)
    
    # Configuração das dimensões
    dims_config = [
        ("dim_paciente.csv", "paciente_id", "Paciente"),
        ("dim_localizacao.csv", "localizacao_id", "Localizacao"),
        ("dim_instituicao.csv", "instituicao_id", "Instituicao"),
        ("dim_tumor.csv", "tumor_id", "Tumor"),
        ("dim_fatores_risco.csv", "fatores_id", "Fatores de Risco"),
        ("dim_ocupacao.csv", "ocupacao_id", "Ocupacao"),
        ("dim_tempo.csv", "tempo_id", "Tempo"),
        ("dim_tratamento.csv", "tratamento_id", "Tratamento"),
    ]
    
    # Validar cada dimensão
    print("\n2. Validando correspondencia de hashes...")
    print("-" * 70)
    
    resultados = []
    all_ok = True
    
    for dim_file, fact_col, dim_name in dims_config:
        dim_path = dimensions_dir / dim_file
        
        # Verificar se dimensão existe
        if not dim_path.exists():
            print(f"   [ERRO] {dim_name}: Arquivo {dim_file} nao encontrado!")
            all_ok = False
            continue
        
        # Carregar dimensão
        try:
            df_dim = pd.read_csv(dim_path)
        except Exception as e:
            print(f"   [ERRO] {dim_name}: Erro ao carregar {dim_file}: {e}")
            all_ok = False
            continue
        
        # Validar
        sucesso, taxa, total_fato, total_dim, mensagem = validate_dimension(
            df_fact, df_dim, fact_col, dim_name
        )
        
        # Armazenar resultado
        resultados.append({
            "dimensao": dim_name,
            "sucesso": sucesso,
            "taxa": taxa,
            "total_fato": total_fato,
            "total_dim": total_dim,
            "arquivo": dim_file
        })
        
        # Imprimir resultado
        status = "[OK]  " if sucesso else "[ERRO]"
        print(f"   {status} {mensagem}")
        
        # Se houver erro, mostrar exemplos
        if not sucesso:
            all_ok = False
            print_hash_examples(df_fact, df_dim, fact_col, dim_name)
    
    # Resumo final
    print("\n" + "=" * 70)
    print("RESUMO DA VALIDACAO")
    print("=" * 70)
    
    if all_ok:
        print("\n✓✓✓ VALIDACAO CONCLUIDA COM SUCESSO! ✓✓✓")
        print("\nTodos os hashes batem 100%!")
        print("Os JOINs entre fato e dimensoes funcionarao corretamente.")
        print("\nProximo passo: python scripts/load_to_supabase.py")
    else:
        print("\n✗✗✗ VALIDACAO FALHOU! ✗✗✗")
        print("\nAlguns hashes nao batem. Os JOINs NAO funcionarao!")
        print("\n ACOES NECESSARIAS:")
        print("   1. Verifique se etl_dimensions.py e etl_fact.py usam as MESMAS funcoes de hash")
        print("   2. Delete todos os CSVs em dimensions/")
        print("   3. Execute: python scripts/etl_dimensions.py")
        print("   4. Execute: python scripts/etl_fact.py")
        print("   5. Execute novamente: python scripts/validate_integrity.py")
    
    # Estatísticas detalhadas
    print("\n" + "-" * 70)
    print("ESTATISTICAS DETALHADAS")
    print("-" * 70)
    print(f"{'Dimensao':<20} {'Taxa Match':>12} {'Hashes Fato':>15} {'Hashes Dim':>15}")
    print("-" * 70)
    
    for r in resultados:
        taxa_str = f"{r['taxa']:.1f}%"
        print(f"{r['dimensao']:<20} {taxa_str:>12} {r['total_fato']:>15,} {r['total_dim']:>15,}")
    
    print("-" * 70)
    
    taxa_media = sum(r['taxa'] for r in resultados) / len(resultados) if resultados else 0
    print(f"{'MEDIA':<20} {taxa_media:>11.1f}%")
    
    print("=" * 70)
    
    # Retornar código de saída
    if all_ok:
        print("\n[SUCCESS] Validacao concluida com sucesso!")
        sys.exit(0)
    else:
        print("\n[FAILURE] Validacao falhou! Corrija os erros acima.")
        sys.exit(1)


if __name__ == "__main__":
    main()


