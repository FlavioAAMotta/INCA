"""
Criar Mapeamento: hash_key → id gerado pelo Supabase
Este script cria tabelas temporárias que mapeiam as hash_keys para os IDs.
"""

import os
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv


def create_mapping_for_dimension(conn, dim_name: str, csv_file: Path):
    """
    Cria uma tabela de mapeamento hash_key → id para uma dimensão.
    """
    print(f"\n   Criando mapeamento para {dim_name}...")
    
    # Carregar CSV com hash_keys
    df_csv = pd.read_csv(csv_file)
    
    if 'hash_key' not in df_csv.columns:
        print(f"      ERRO: hash_key nao encontrada no CSV")
        return
    
    # Buscar IDs gerados pelo Supabase
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {dim_name} ORDER BY id;")
        ids_supabase = [row[0] for row in cur.fetchall()]
    
    if len(ids_supabase) != len(df_csv):
        print(f"      AVISO: Quantidades diferentes!")
        print(f"         CSV: {len(df_csv):,} registros")
        print(f"         Supabase: {len(ids_supabase):,} registros")
        return
    
    # Criar tabela de mapeamento
    mapping_table = f"map_{dim_name}"
    
    with conn.cursor() as cur:
        # Dropar se existir
        cur.execute(f"DROP TABLE IF EXISTS {mapping_table};")
        
        # Criar nova tabela
        cur.execute(f"""
            CREATE TABLE {mapping_table} (
                hash_key VARCHAR(32) PRIMARY KEY,
                original_id INTEGER
            );
        """)
        
        # Inserir mapeamentos
        for hash_key, supabase_id in zip(df_csv['hash_key'], ids_supabase):
            cur.execute(f"""
                INSERT INTO {mapping_table} (hash_key, original_id)
                VALUES (%s, %s);
            """, (hash_key, supabase_id))
        
        # Criar índice
        cur.execute(f"CREATE INDEX idx_{mapping_table}_hash ON {mapping_table}(hash_key);")
    
    conn.commit()
    print(f"      [OK] {len(ids_supabase):,} mapeamentos criados")


def main():
    """Cria todas as tabelas de mapeamento."""
    print("="*60)
    print("CRIACAO DE TABELAS DE MAPEAMENTO")
    print("="*60)
    
    load_dotenv()
    
    # Conectar
    print("\n1. Conectando ao Supabase...")
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        database=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=os.getenv("SUPABASE_PORT", 5432)
    )
    print("   [OK] Conectado")
    
    dimensions_dir = Path("dimensions")
    
    # Mapear cada dimensão
    print("\n2. Criando mapeamentos...")
    
    dimensions = [
        ("dim_paciente", dimensions_dir / "dim_paciente.csv"),
        ("dim_localizacao", dimensions_dir / "dim_localizacao.csv"),
        ("dim_instituicao", dimensions_dir / "dim_instituicao.csv"),
        ("dim_tumor", dimensions_dir / "dim_tumor.csv"),
        ("dim_fatores_risco", dimensions_dir / "dim_fatores_risco.csv"),
        ("dim_ocupacao", dimensions_dir / "dim_ocupacao.csv"),
        ("dim_tempo", dimensions_dir / "dim_tempo.csv"),
        ("dim_tratamento", dimensions_dir / "dim_tratamento.csv")
    ]
    
    try:
        for dim_name, csv_file in dimensions:
            create_mapping_for_dimension(conn, dim_name, csv_file)
        
        print("\n" + "="*60)
        print("MAPEAMENTOS CRIADOS COM SUCESSO!")
        print("="*60)
        print("\nTabelas criadas:")
        for dim_name, _ in dimensions:
            print(f"   - map_{dim_name}")
        
        print("\nAgora você pode usar JOINs para converter hash_keys em IDs")
        
    except Exception as e:
        print(f"\nERRO: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    
    finally:
        conn.close()
        print("\nConexao encerrada")


if __name__ == "__main__":
    main()

