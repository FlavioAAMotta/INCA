import pandas as pd
from dbfread import DBF
import os
import re
import glob

def ler_arquivo_cnv(caminho_arquivo):
    """
    Lê um arquivo .cnv e retorna um dicionário com o mapeamento código -> descrição
    """
    mapeamento = {}
    
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as arquivo:
            linhas = arquivo.readlines()
            
        # Pular as primeiras linhas de cabeçalho e encontrar os dados
        for i, linha in enumerate(linhas):
            linha_original = linha
            linha = linha.strip()
            
            # Procurar por linhas que começam com espaços seguidos de número
            if re.match(r'^\s+\d+\s+', linha_original):
                # Usar a linha original para preservar espaçamento
                # Formato: "      1  Descrição                                          1"
                match = re.match(r'^\s+(\d+)\s+(.+?)\s+(\d+)\s*$', linha_original)
                if match:
                    seq_num = int(match.group(1))
                    descricao = match.group(2).strip()
                    codigo = int(match.group(3))
                    mapeamento[codigo] = descricao
                else:
                    # Tentar formato alternativo com split por espaços múltiplos
                    partes = re.split(r'\s{2,}', linha.strip())
                    if len(partes) >= 3:
                        try:
                            seq_num = int(partes[0])
                            descricao = partes[1]
                            codigo = int(partes[2])
                            mapeamento[codigo] = descricao
                        except ValueError:
                            continue
                    
    except Exception as e:
        print(f"Erro ao ler arquivo {caminho_arquivo}: {e}")
        
    return mapeamento


def extrair_mapeamentos_def(caminho_def):
    """
    Extrai os mapeamentos de colunas para arquivos .cnv do arquivo .def
    """
    mapeamentos = {}
    
    try:
        with open(caminho_def, 'r', encoding='latin1') as arquivo:
            linhas = arquivo.readlines()
            
        for linha in linhas:
            linha = linha.strip()
            # Procurar por linhas que definem mapeamentos (começam com T ou S)
            if linha.startswith(('T', 'S')) and '.cnv' in linha:
                partes = linha.split(',')
                if len(partes) >= 4:
                    nome_coluna = partes[1].strip()
                    arquivo_cnv = partes[3].strip()
                    # Extrair apenas o nome do arquivo .cnv
                    nome_arquivo = os.path.basename(arquivo_cnv)
                    if nome_coluna not in mapeamentos:
                        mapeamentos[nome_coluna] = nome_arquivo
                        
    except Exception as e:
        print(f"Erro ao ler arquivo .def: {e}")
        
    return mapeamentos


def criar_mapeamentos_completos():
    """
    Cria dicionário completo com todos os mapeamentos
    """
    return {
        'SEXO': {
            1: 'Masculino',
            2: 'Feminino',
            3: 'Não informado',
            9: 'Não informado'
        },
        'ALCOOLIS': {
            1: 'Nunca',
            2: 'Ex-consumidor', 
            3: 'Sim',
            4: 'Nao avaliado',
            8: 'Nao se aplica',
            9: 'Sem Informacao'
        },
        'TPCASO': {
            1: 'Analitico',
            2: 'Nao Analitico'
        },
        'RACACOR': {
            1: 'Branca',
            2: 'Preta',
            3: 'Amarela',
            4: 'Parda',
            5: 'Indigena',
            9: 'Sem Informacao'
        },
        'TABAGISM': {
            1: 'Nunca',
            2: 'Ex-consumidor',
            3: 'Sim',
            4: 'Nao avaliado',
            8: 'Nao se aplica',
            9: 'Sem Informacao'
        },
        'INSTRUC': {
            1: 'Nenhuma',
            2: 'Fundamental incompleto',
            3: 'Fundamental completo',
            4: 'Nivel medio',
            5: 'Nivel superior incompleto',
            6: 'Nivel superior completo',
            9: 'Sem informacao'
        },
        'ESTCONJ': {
            1: 'SOLTEIRO',
            2: 'CASADO',
            3: 'VIUVO',
            4: 'SEPARADO JUDICIALMENTE',
            5: 'UNIAO CONSENSUAL',
            9: 'SEM INFORMACAO'
        },
        'HISTFAMC': {
            1: 'Nao',
            2: 'Sim',
            9: 'Sem informacao'
        },
        'ORIENC': {
            1: 'SUS',
            2: 'Nao SUS',
            3: 'Veio por conta propria',
            8: 'Nao se aplica',
            9: 'Sem informacao'
        },
        'EXDIAG': {
            1: 'Exame Clinico e Patologia Clinica',
            2: 'Exames por Imagem',
            3: 'Endoscopia e Cirurgia Exploradora',
            4: 'Anatomia Patologica',
            5: 'Marcadores Tumorais',
            8: 'Nao se aplica',
            9: 'Sem Informacao'
        },
        'DIAGANT': {
            1: 'Sem Diagnostico Previo',
            2: 'Diagnosticado sem Tratamento',
            3: 'Encaminhado Apos Inicio Tratamento',
            8: 'Nao se aplica',
            9: 'Sem Informacao'
        },
        'MAISUMTU': {
            1: 'Nao',
            2: 'Sim',
            8: 'Nao se aplica',
            9: 'Sem informacao'
        },
        'LATERALI': {
            0: 'Nao se aplica',
            1: 'Direita',
            2: 'Esquerda',
            3: 'Bilateral',
            9: 'Sem informacao'
        },
        'BASMAIMP': {
            1: 'Clinica',
            2: 'Clinica + Exames',
            3: 'Exames',
            9: 'Sem informacao'
        }
    }


def aplicar_mapeamentos(df, mapeamentos):
    """
    Aplica os mapeamentos ao DataFrame
    """
    colunas_mapeadas = []
    
    for coluna, mapeamento in mapeamentos.items():
        if coluna in df.columns:
            try:
                # Aplicar mapeamento com múltiplos métodos para garantir funcionamento
                # Método 1: replace direto
                df[coluna] = df[coluna].replace(mapeamento)
                
                # Método 2: map com fallback
                df_temp = df[coluna].map(mapeamento)
                df[coluna] = df_temp.fillna(df[coluna])
                
                # Método 3: conversão string + replace
                df[coluna] = df[coluna].astype(str).replace({str(k): v for k, v in mapeamento.items()})
                
                colunas_mapeadas.append(coluna)
                
            except Exception as e:
                print(f"  ⚠️ Erro ao mapear coluna {coluna}: {e}")
    
    return df, colunas_mapeadas


def processar_dbf(arquivo_dbf, pasta_saida='saida'):
    """
    Processa um arquivo DBF e salva como CSV com mapeamentos aplicados
    """
    try:
        # Criar pasta de saída se não existir
        if not os.path.exists(pasta_saida):
            os.makedirs(pasta_saida)
        
        # Nome do arquivo de saída
        nome_base = os.path.basename(arquivo_dbf)
        nome_csv = nome_base.replace('.dbf', '.csv')
        caminho_csv = os.path.join(pasta_saida, nome_csv)
        
        print(f"\n{'='*60}")
        print(f"Processando: {nome_base}")
        print(f"{'='*60}")
        
        # Ler o arquivo DBF
        print("  Lendo arquivo DBF...")
        df = pd.DataFrame(list(DBF(arquivo_dbf, encoding='latin1')))
        print(f"  OK - Arquivo lido: {len(df)} linhas, {len(df.columns)} colunas")
        
        # Aplicar mapeamentos
        print("  Aplicando mapeamentos...")
        mapeamentos = criar_mapeamentos_completos()
        df, colunas_mapeadas = aplicar_mapeamentos(df, mapeamentos)
        
        if colunas_mapeadas:
            print(f"  OK - {len(colunas_mapeadas)} colunas mapeadas: {', '.join(colunas_mapeadas)}")
        else:
            print("  AVISO - Nenhuma coluna foi mapeada")
        
        # Salvar como CSV
        print(f"  Salvando CSV: {nome_csv}")
        df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        print(f"  OK - Arquivo salvo com sucesso!")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Erro ao processar {arquivo_dbf}: {e}")
        return False


def main():
    """
    Função principal - processa todos os arquivos .dbf da pasta
    """
    print("PROCESSAMENTO DE ARQUIVOS DBF")
    print("="*60)
    
    # Buscar todos os arquivos .dbf na pasta atual
    arquivos_dbf = glob.glob('*.dbf')
    
    if not arquivos_dbf:
        print("ERRO: Nenhum arquivo .dbf encontrado na pasta atual!")
        return
    
    print(f"Encontrados {len(arquivos_dbf)} arquivos .dbf")
    print(f"   Arquivos: {', '.join([os.path.basename(f) for f in arquivos_dbf[:5]])}")
    if len(arquivos_dbf) > 5:
        print(f"   ... e mais {len(arquivos_dbf) - 5} arquivos")
    
    # Processar cada arquivo
    sucessos = 0
    falhas = 0
    
    for arquivo in arquivos_dbf:
        if processar_dbf(arquivo):
            sucessos += 1
        else:
            falhas += 1
    
    # Resumo final
    print(f"\n{'='*60}")
    print("RESUMO FINAL")
    print(f"{'='*60}")
    print(f"  Arquivos processados com sucesso: {sucessos}")
    print(f"  Arquivos com erro: {falhas}")
    print(f"  Taxa de sucesso: {sucessos/len(arquivos_dbf)*100:.1f}%")
    print(f"\n  Arquivos CSV salvos na pasta: 'saida'")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

