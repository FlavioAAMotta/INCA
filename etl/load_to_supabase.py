"""
ETL - Carga para Supabase (PostgreSQL)
Realiza a carga dos dados CSV para o banco Supabase via COPY.
Carrega dimensões primeiro, depois a tabela fato com mapeamento de IDs.
"""

import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import json
from datetime import datetime


def load_checkpoint(checkpoint_file: Path) -> dict:
    """Carrega checkpoint do carregamento."""
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return {"loaded_tables": [], "last_update": None}


def save_checkpoint(checkpoint_file: Path, checkpoint_data: dict):
    """Salva checkpoint do carregamento."""
    checkpoint_data["last_update"] = datetime.now().isoformat()
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)


def truncate_table(conn, table_name: str):
    """Limpa uma tabela (opcional - cuidado!)"""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
    conn.commit()
    print(f"   Tabela {table_name} truncada")


def load_dimension(conn, table_name: str, csv_file: Path, checkpoint_file: Path, checkpoint: dict):
    """
    Carrega uma dimensão para o Supabase.
    O campo hash_key do CSV não é inserido - o Supabase gera o ID automaticamente.
    
    Args:
        conn: Conexão ativa do psycopg2
        table_name: Nome da tabela destino
        csv_file: Caminho do arquivo CSV local
        checkpoint_file: Arquivo de checkpoint
        checkpoint: Dados do checkpoint
    """
    if table_name in checkpoint["loaded_tables"]:
        print(f"   [SKIP] {table_name} - ja carregado")
        return
    
    print(f"\n   Carregando: {table_name}")

    if not csv_file.exists():
        print(f"      ERRO: Arquivo {csv_file} nao encontrado!")
        return

    import pandas as pd
    from io import StringIO
    
    # Carregar CSV com pandas
    df = pd.read_csv(csv_file)
    print(f"      Registros no CSV: {len(df):,}")
    
    # Remover coluna hash_key (não existe na tabela Supabase)
    if 'hash_key' in df.columns:
        df = df.drop(columns=['hash_key'])
    
    # Limpeza de dados específica para cada tabela
    if table_name == 'dim_paciente':
        # Limpar campo idade: "0-1" -> 0, valores inválidos -> NULL
        if 'idade' in df.columns:
            # Converter "0-1" para 0
            df['idade'] = df['idade'].astype(str).str.replace('0-1', '0')
            # Converter para numérico, inválidos viram NaN
            df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
            # Converter NaN para None (NULL no Postgres)
            df['idade'] = df['idade'].where(pd.notna(df['idade']), None)
            
            invalidos = df['idade'].isna().sum()
            if invalidos > 0:
                print(f"      AVISO: {invalidos} registros com idade invalida convertidos para NULL")
        
        # Limpar campo estado_civil: "0" -> NULL (sem informação)
        if 'estado_civil' in df.columns:
            df.loc[df['estado_civil'] == '0', 'estado_civil'] = None
            nulos = df['estado_civil'].isna().sum()
            if nulos > 0:
                print(f"      INFO: {nulos} registros com estado_civil desconhecido (NULL)")
    
    # Colunas restantes
    columns = df.columns.tolist()
    print(f"      Colunas: {', '.join(columns)}")
    
    # Converter DataFrame para CSV em memória (sem hash_key)
    # Substituir NaN/None por string vazia para o COPY (será NULL no Postgres)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False, na_rep='')
    csv_buffer.seek(0)
    
    with conn.cursor() as cur:
        # Fazer COPY direto para a tabela
        # O Postgres vai gerar o ID (SERIAL) automaticamente
        cur.copy_expert(
            sql=f"""
                COPY {table_name}({','.join(columns)})
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ',', NULL '', ENCODING 'UTF8');
            """,
            file=csv_buffer
        )
        
        # Verificar quantidade de registros inseridos
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cur.fetchone()[0]
        
        print(f"      [OK] {count:,} registros inseridos")

    conn.commit()
    
    # Atualizar checkpoint
    checkpoint["loaded_tables"].append(table_name)
    save_checkpoint(checkpoint_file, checkpoint)
    print(f"      Checkpoint atualizado")


def create_hash_mapping_table(conn, dim_table: str, csv_file: Path):
    """
    Cria uma tabela temporária que mapeia hash_key -> id gerado pelo Supabase.
    Isso permite fazer o JOIN na tabela fato.
    """
    print(f"\n   Criando mapeamento hash->id para {dim_table}...")
    
    with conn.cursor() as cur:
        # Criar tabela de mapeamento
        mapping_table = f"temp_map_{dim_table}"
        
        cur.execute(f"""
            CREATE TEMP TABLE {mapping_table} (
                hash_key TEXT,
                generated_id INT
            );
        """)
        
        # Carregar CSV com hash_key
        import pandas as pd
        df = pd.read_csv(csv_file)
        
        if 'hash_key' not in df.columns:
            print(f"      AVISO: hash_key nao encontrada em {csv_file.name}")
            return
        
        # Buscar IDs gerados pelo banco
        # Assumindo que a ordem de inserção é mantida
        cur.execute(f"SELECT id FROM {dim_table} ORDER BY id;")
        ids = [row[0] for row in cur.fetchall()]
        
        if len(ids) != len(df):
            print(f"      ERRO: Quantidade de IDs ({len(ids)}) != registros no CSV ({len(df)})")
            return
        
        # Inserir mapeamento
        for hash_key, gen_id in zip(df['hash_key'], ids):
            cur.execute(f"""
                INSERT INTO {mapping_table} (hash_key, generated_id)
                VALUES (%s, %s);
            """, (hash_key, gen_id))
        
        conn.commit()
        print(f"      [OK] {len(ids):,} mapeamentos criados")


def load_fact_table(conn, csv_file: Path):
    """
    Carrega a tabela fato usando COPY (mais rápido que INSERT).
    Processa o CSV em chunks para evitar problemas de memória.
    """
    print(f"\n   Carregando tabela fato...")
    
    if not csv_file.exists():
        print(f"      ERRO: Arquivo {csv_file} nao encontrado!")
        return
    
    import pandas as pd
    from io import StringIO
    
    # Ler apenas o cabeçalho para identificar as colunas
    print(f"      Lendo estrutura do CSV...")
    df_sample = pd.read_csv(csv_file, nrows=1)
    
    # Remover colunas geradas (GENERATED COLUMNS) - não podem ser inseridas
    generated_columns = ['sobreviveu']  # Coluna calculada: (data_obito IS NULL)
    
    columns_to_drop = [col for col in generated_columns if col in df_sample.columns]
    if columns_to_drop:
        df_sample = df_sample.drop(columns=columns_to_drop)
        print(f"      Removendo colunas geradas: {', '.join(columns_to_drop)}")
    
    # Colunas que serão inseridas
    columns = df_sample.columns.tolist()
    print(f"      Colunas para COPY: {len(columns)}")
    
    # Processar CSV em chunks diretamente (sem carregar tudo na memória)
    chunk_size = 50000  # Reduzido para evitar problemas de memória
    print(f"      Processando em chunks de {chunk_size:,} registros...")
    
    chunk_num = 0
    total_rows = 0
    
    # Ler CSV em chunks usando chunksize
    try:
        chunk_iterator = pd.read_csv(
            csv_file,
            chunksize=chunk_size,
            low_memory=False,
            dtype=str  # Ler tudo como string primeiro para evitar problemas de tipo
        )
        
        for chunk in chunk_iterator:
            chunk_num += 1
            
            # Remover colunas geradas do chunk
            for col in columns_to_drop:
                if col in chunk.columns:
                    chunk = chunk.drop(columns=[col])
            
            # Garantir que as colunas estão na mesma ordem
            chunk = chunk[columns]
            
            # Substituir NaN por None (será NULL no Postgres)
            chunk = chunk.where(pd.notna(chunk), None)
            
            print(f"      [Chunk {chunk_num}] Processando {len(chunk):,} registros...", end=" ")
            
            # Converter chunk para CSV em memória
            csv_buffer = StringIO()
            chunk.to_csv(csv_buffer, index=False, header=False, na_rep='')
            csv_buffer.seek(0)
            
            # Fazer COPY
            with conn.cursor() as cur:
                try:
                    cur.copy_expert(
                        sql=f"""
                            COPY fato_casos_oncologicos({','.join(columns)})
                            FROM STDIN
                            WITH (FORMAT CSV, DELIMITER ',', NULL '', ENCODING 'UTF8');
                        """,
                        file=csv_buffer
                    )
                    conn.commit()
                    total_rows += len(chunk)
                    print(f"[OK] Total: {total_rows:,}")
                except Exception as e:
                    print(f"\n      ERRO no chunk {chunk_num}: {e}")
                    conn.rollback()
                    raise
            
            # Liberar memória
            del chunk
            del csv_buffer
    
    except Exception as e:
        print(f"\n      ERRO durante processamento: {e}")
        conn.rollback()
        raise
    
    # Verificar total inserido
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fato_casos_oncologicos;")
        count = cur.fetchone()[0]
        print(f"\n      [CONCLUIDO] {count:,} registros na tabela fato")


def main():
    """Carrega dimensões e tabela fato no Supabase."""
    print("="*60)
    print("CARGA PARA SUPABASE")
    print("="*60)
    
    load_dotenv()

    # Verificar variáveis de ambiente
    required_vars = ["SUPABASE_HOST", "SUPABASE_DB", "SUPABASE_USER", "SUPABASE_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("\nERRO: Variaveis de ambiente faltando:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nCrie um arquivo .env com as credenciais do Supabase")
        print("Use supabase.env.example como referencia")
        return

    # Conectar ao Supabase
    print("\n1. Conectando ao Supabase...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST"),
            database=os.getenv("SUPABASE_DB"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            port=os.getenv("SUPABASE_PORT", 5432)
        )
        print("   [OK] Conexao estabelecida")
    except Exception as e:
        print(f"\n   ERRO ao conectar: {e}")
        print("\nVerifique:")
        print("   1. Se as credenciais no .env estao corretas")
        print("   2. Se o IP esta liberado no Supabase (Settings > Database > Connection pooling)")
        print("   3. Se a porta 5432 esta aberta no firewall")
        return

    dimensions_dir = Path("dimensions")
    facts_dir = Path("facts")
    checkpoint_file = Path("load_checkpoint.json")

    # Carregar checkpoint
    checkpoint = load_checkpoint(checkpoint_file)
    
    if checkpoint["loaded_tables"]:
        print(f"\nCheckpoint encontrado!")
        print(f"   Tabelas ja carregadas: {len(checkpoint['loaded_tables'])}")
        print(f"   Ultimo carregamento: {checkpoint['last_update']}")

    try:
        print("\n" + "="*60)
        print("2. Carregando dimensoes...")
        print("="*60)
        
        # Ordem de carga: dimensões não dependentes primeiro
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
        
        for table_name, csv_file in dimensions:
            load_dimension(conn, table_name, csv_file, checkpoint_file, checkpoint)

        print("\n" + "="*60)
        print("3. Dimensoes carregadas com sucesso!")
        print("="*60)
        
        # Carregar tabela fato
        print("\n" + "="*60)
        print("4. Carregando tabela fato...")
        print("="*60)
        print("\nAVISO: A tabela fato possui ~3.7 milhoes de registros")
        print("       Este processo pode demorar 10-30 minutos")
        print("       Use hash_keys como foreign keys temporariamente")
        print("\nDeseja continuar? (s/n): ", end="")
        
        # Para automação, comentar a linha abaixo
        # response = input().lower()
        # if response != 's':
        #     print("\nCarga da tabela fato cancelada")
        #     return
        
        
        load_fact_table(conn, facts_dir / "fato_casos_oncologicos.csv")

        print("\n" + "="*60)
        print("CARGA CONCLUIDA!")
        print("="*60)
        print(f"\nDimensoes carregadas: {len(checkpoint['loaded_tables'])}")
        print("\nProximos passos:")
        print("   1. Verificar os dados no Supabase")
        print("   2. Criar indices nas colunas de busca")
        print("   3. Descomentar carga da tabela fato no script")
        print("="*60)

    except Exception as e:
        print(f"\nERRO durante a carga: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()

    finally:
        conn.close()
        print("\nConexao encerrada.")


if __name__ == "__main__":
    main()
