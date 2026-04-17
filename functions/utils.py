from django.db import connection
import os, csv, traceback
import zipfile, unicodedata, re, pandas as pd
from datetime import datetime
from django.contrib import messages
import requests, pytz,chardet
from users.models import *
# from universal_data.models import *
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
# from universal_data.models import salva_log
import warnings
warnings.filterwarnings('ignore')
import time
import numpy as np
from pathlib import Path
from data.models import salva_dado
import traceback
from concurrent.futures import ProcessPoolExecutor
from typing import List, Tuple

PASTA_ARQUIVOS_BLACKLIST = os.path.join(os.getcwd(), "media/arquivos_blacklist")
PASTA_ARQUIVOS_TELS_NEXT = os.path.join(os.getcwd(), "media/arquivos_tels_next")
COLUNAS_DFV=["UF",	"MUNICIPIO",	"LOCALIDADE",	"BAIRRO",	"LOGRADOURO",	"CEP",	"CELULA",	"TIPO_CDO",	"COMPLEMENTO2",	"COMPLEMENTO3",	"CODIGO_LOGRADOURO",	"NO_FACHADA",	"COMPLEMENTO1",	"VIABILIDADE_ATUAL",	"HP_TOTAL",	"HP_LIVRE",	"OPB_CEL",	"DT_ATUALIZACAO",]
COLUNAS_ARQUIVO_CREDITO = ["REGIONAL,"	"PDV,"	"CNPJ,"	"CPF,"	"CREDITO_PREAPROVADO,"	"CREDITO_MOTIVO_NEGACAO,"	"DISP_FTTH,"]
COLUNAS_DADOS_ENRIQUECIMENTO = ["CNPJ","RAZAO_SOCIAL","ENDERECO","NUMERO","COMPLEMENTO","BAIRRO","CIDADE","UF","CEP","FIXO1","FIXO2","FIXO3","CELULAR1","CELULAR2","CELULAR3","PLACA1","MARCA/MODELO1","RENAVAM1","CHASSI1","PLACA2","MARCA/MODELO2","RENAVAM2","CHASSI2","PLACA3","MARCA/MODELO3","RENAVAM3","CHASSI3","DATA_ABERTURA","EMAIL1","EMAIL2","EMAIL3","CAPITAL_SOCIAL","COD_CNAE","DESCRICAO_CNAE","CPF_SOCIO1","NOME_SOCIO1","CARGO_SOCIO1","ENDERECO_SOCIO1","NUMERO_SOCIO1","COMPLEMENTO_SOCIO1","BAIRRO_SOCIO1","CIDADE_SOCIO1","UF_SOCIO1","CEP_SOCIO1","TELEFONE1_SOCIO1","TELEFONE2_SOCIO1","TELEFONE3_SOCIO1","CPF_SOCIO2","NOME_SOCIO2","CARGO_SOCIO2","ENDERECO_SOCIO2","NUMERO_SOCIO2","COMPLEMENTO_SOCIO2","BAIRRO_SOCIO2","CIDADE_SOCIO2","UF_SOCIO2","CEP_SOCIO2","TELEFONE1_SOCIO2","TELEFONE2_SOCIO2","TELEFONE3_SOCIO2","CPF_SOCIO3","NOME_SOCIO3","CARGO_SOCIO3","ENDERECO_SOCIO3","NUMERO_SOCIO3","COMPLEMENTO_SOCIO3","BAIRRO_SOCIO3","CIDADE_SOCIO3","UF_SOCIO3","CEP_SOCIO3","TELEFONE1_SOCIO3","TELEFONE2_SOCIO3","TELEFONE3_SOCIO3","QTD_FUNCIONARIO","TIPO_FILIAL","NATUREZA_EMPRESA","DESCRICAO_NATUREZA","PORTE_EMPRESA"]
tz_sp = pytz.timezone("America/Sao_Paulo")

def ler_arquivos_e_coletar_telefones(sistema:str="oi"):
    telefones_blacklist = []
    telefones_tels_next = []

    def processar_arquivo(caminho, tipo) -> list:
        telefones_arquivo = []
        try:
            extensao = os.path.splitext(caminho)[1].lower()
            
            # Ler arquivo conforme a extensão
            if extensao == '.csv':
                with open(caminho, 'r') as f:
                    dialeto = csv.Sniffer().sniff(f.read(1024))
                df = pd.read_csv(caminho, sep=dialeto.delimiter, dtype=str)
            elif extensao in ('.xls', '.xlsx', ".xlsb"):
                df = pd.read_excel(caminho, dtype=str)
            elif extensao == '.txt':
                df = pd.read_csv(caminho, sep=';', dtype=str)
            else:
                return

            # Padronizar nomes de colunas
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

            # Identificar colunas com telefone
            if tipo == "blacklist":
                colunas_telefone = [col for col in df.columns if 'telefone' in col or "ddd" in col]
            else:
                colunas_telefone = [col for col in df.columns if 'numero' in col or 'telefone' in col]
 
            # Adicionar números ao conjunto
            for coluna in colunas_telefone:
                nums = df[coluna].dropna().astype(str).str.strip().to_list()
                telefones_arquivo+=nums
            return telefones_arquivo

        except Exception as e: #tentando ler agora como lista em texto
            try:
                with open(caminho, "r", encoding="latin-1") as arq:
                    numeros = [clean_phone_number(numb) for numb in arq.read().split("\n")]
                return numeros
            except:
                return []
            
    # Processar todos os arquivos na pasta atual
    for arq in os.listdir(PASTA_ARQUIVOS_BLACKLIST):
        arquivo = os.path.join(PASTA_ARQUIVOS_BLACKLIST, arq)
        if os.path.isfile(arquivo):
            news_tels=processar_arquivo(arquivo, "blacklist")
            if type(news_tels) == list:
                telefones_blacklist+=news_tels

    telefones_blacklist = list(set(telefones_blacklist))

    if sistema != "oi":
        return telefones_blacklist
    
    for arq in os.listdir(PASTA_ARQUIVOS_TELS_NEXT):
        arquivo = os.path.join(PASTA_ARQUIVOS_TELS_NEXT, arq)
        if os.path.isfile(arquivo):
            news_tels=processar_arquivo(arquivo, "tels_next")
            if type(news_tels) == list:
                telefones_tels_next+=news_tels
    
    telefones_tels_next = list(set(telefones_tels_next))

    return list(set(telefones_tels_next + telefones_blacklist))

def analisa_quarentena(raiz):

    arquivo_quarentena = os.path.join(raiz, f"arquivos_quarentena/quarentena.csv")

    if not os.path.exists(arquivo_quarentena):
        return []
    
    
    # Lê o arquivo CSV mantendo todas as colunas como strings
    df = pd.read_csv(arquivo_quarentena, sep=';', dtype=str)

    if len(df.index) < 2:
        return []
    
    # Converte a coluna 'Data/Hora' para datetime
    df['DataHora_dt'] = pd.to_datetime(df['Data/Hora'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    
    # Converte a coluna 'Quarentena' para valores numéricos
    df['Quarentena_int'] = pd.to_numeric(df['Quarentena'], errors='coerce')
    
    # Calcula a data final da quarentena (data/hora + dias de quarentena)
    df['Quarentena_End'] = df['DataHora_dt'] + pd.to_timedelta(df['Quarentena_int'], unit='D')
    
    # Obtém a data/hora atual
    now = pd.Timestamp.now()
    
    # Filtra as linhas onde a quarentena ainda não terminou
    mask = df['Quarentena_End'] > now
    df_filtrado = df[mask].copy()
    
    # Mantém apenas as colunas originais
    df_filtrado = df_filtrado[['Data/Hora', 'Quarentena', 'Telefone']]
    
    # Sobrescreve o arquivo CSV com os dados filtrados
    df_filtrado.to_csv(arquivo_quarentena, sep=';', index=False)
    
    # Retorna a lista de telefones dos registros restantes
    return df_filtrado['Telefone'].tolist()

def _detectar_sep_csv(caminho: str, encoding: str = "latin-1", amostra: int = 4096):
    """Tenta inferir o separador (“delimiter”) de um CSV/TXT.

    Retorna o separador detectado (str). Se não conseguir, devolve ';' como padrão.
    """
    with open(caminho, "r", encoding=encoding, newline="") as f:
        sample = f.read(amostra)

    try:
        # Testa apenas separadores mais comuns para evitar falsos-positivos
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        # Fallback bem trivial: conta qual char aparece mais na primeira linha
        primeira_linha = sample.splitlines()[0] if sample else ""
        candidatos = [",", ";", "\t", "|"]
        frequencias = {c: primeira_linha.count(c) for c in candidatos}
        mais_comum = max(frequencias, key=frequencias.get)
        return mais_comum if frequencias[mais_comum] > 0 else ";"

def _detectar_encoding_csv(caminho, amostra=16000):
    with open(caminho, 'rb') as f:
        rawdata = f.read(amostra)
    resultado = chardet.detect(rawdata)
    encoding = resultado['encoding']
    # Prioriza utf-8 se for aceito ou default conhecido
    if encoding is None:
        return 'latin-1'  # fallback genérico
    if encoding.lower().replace('-', '') in ['utf8', 'utf']:
        return 'utf-8'
    if encoding.lower() == 'ascii':
        return 'latin-1'
    # Latin-1 raramente falha na leitura, mas pode mascarar problemas
    return encoding

def remove_fixos(df: pd.DataFrame, colunas_telefone: list = ["TEL1", "TEL2", "TEL3"]) -> pd.DataFrame:
    # 1. Limpa todos os números (mantenha sua lógica)
    for col in colunas_telefone:
        df[col] = df[col].apply(lambda x: clean_phone_number(x, True))
    
    # 2. Reorganiza os números puxando para a esquerda
    def shift_phones(row):
        # Lista com apenas telefones válidos (não vazios/não nulos)
        phones = [row[col] for col in colunas_telefone if pd.notnull(row[col]) and str(row[col]).strip() != ""]
        # Preenche até o número de colunas
        phones += [""] * (len(colunas_telefone) - len(phones))
        # Retorna como Series para atribuir nos nomes corretos
        return pd.Series(phones, index=colunas_telefone)

    df[colunas_telefone] = df.apply(shift_phones, axis=1)
    return df

def clean_phone_number(phone, apenas_celular:bool=False, apenas_fixos:bool=False) -> str:
    """Função para limpar e validar números de telefone"""
    if pd.isna(phone) or str(phone).strip() == '':
        return ''
    # Remove todos os caracteres não numéricos
    cleaned = re.sub(r'\D', '', str(phone))
    if len(cleaned) > 12 and str(cleaned[:2]) == "55":
        cleaned = cleaned[2:]
    ddd = cleaned[:2]

    valid_ddds = ["11","12","13","14","15","16","17","18","19","21","22","24","27","28","31","32","33","34","35","37","38","41","42","43","44","45","46","47","48","49","51","53","54","55","61","62","63","64","65","66","67","68","69","71","73","74","75","77","79","81","82","83","84","85","86","87","88","89","91","92","93","94","95","96"]
    if ddd not in valid_ddds:
        return ""

    #retirando ddds que apresentam problemas no discados URA:
    # if ddd in ["16", "34", "37"]:
    #     return ""

    telefone = cleaned[2:]

    repetidos = [f"{i}"*6 for i in range(0,10)] #Se tiver 6 dígitos repetidos, remove
    if any([rep in telefone for rep in repetidos]):
        return ""

    if len(telefone) == 9: #É celular
        if apenas_fixos:
            return ""

        if str(telefone[0]) == "9": #celular com 9 na frente
            return cleaned
    
        else: #tem 9 dígitos mas não é celular, portanto é um telefone inválido
            return ""
        
    elif len(telefone) == 8:
        if str(telefone[0]) in ["6", "7", "8","9"]: #É celular, mas sem 9 na frente
            return ddd + "9" + telefone
        
        else: #é um telefone fixo válido
            if apenas_celular:
                return ""
            else:
                if telefone[0] == "1":
                    return ""
                return cleaned


    else: #Tamanho inválido para telefone
        return ""

def padronizar_texto(text, separator:str=""):
    """
    Remove acentos, espaços desnecessários e converte para caixa alta.
    """
    if pd.isnull(text):
        return ""
    text = str(text).strip()
    # Remove acentos
    text = ''.join(c for c in unicodedata.normalize('NFD', text) 
                   if unicodedata.category(c) != 'Mn')
    # Converte para caixa alta
    text = text.upper()
    # Substitui múltiplos espaços por um único espaço
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('.0', "")
    text = text.replace(',0', "")
    text = text.replace(';', "")
    text = text.replace(',', "")
    text = text.replace('&', "")
    text = text.replace(':', "")
    text = text.replace('.', "")
    text = text.replace('/', "")
    text = text.replace('-', "")
    if text.upper().strip() == "NAN":
        text = ""
    if text.upper().strip() == "N/A":
        text = ""

    if separator:
        text = text.replace(separator, " ")
    return text

def padronizacao(df: pd.DataFrame, columns=None, separator: str = "") -> pd.DataFrame:
    """
    Padroniza textos em colunas (todas ou selecionadas), garantindo:
      - tratamento seguro de colunas 'category' (add "" e fillna)
      - conversão para string
      - remoção de sufixos ',0' ou '.0'
      - aplicação de padronizar_texto(...)
    """
    df = df.copy()
    cols = df.columns if columns is None else columns

    not_padronize = ["cnpj", "TEL1", "TEL2", "TEL3", "correio_eletronico"]

    for col in cols:
        s = df[col]
        if col not in not_padronize:

            # 1) tratar NaN respeitando category
            if pd.api.types.is_categorical_dtype(s):
                s = s.cat.add_categories([""]).fillna("")
            else:
                # usar dtype 'string' ajuda a padronizar valores ausentes
                s = s.astype("string").fillna("")

            # 2) converter para str e remover sufixos indesejados
            s = s.astype(str).str.replace(r'(,0|\.0)$', '', regex=True)

            # 3) aplicar sua função de normalização
            s = s.apply(lambda x: padronizar_texto(x, separator=separator))

            df[col] = s

    return df

def remover_acentos_series(s: pd.Series) -> pd.Series:

    return (
        s.str.normalize("NFD")
        .str.replace(r"[\u0300-\u036f]", "", regex=True)
    )

def padronizacao_mailing_final(df: pd.DataFrame, columns=None, separator: str = "") -> pd.DataFrame:
    # cols = df.columns if columns is None else columns
    cols = ["bairro", "logradouro", "descricaocf", "razao_social", "nome_fantasia", "decisor"]

    for col in cols:
        ini = time.time()

        s = df[col]

        if pd.api.types.is_categorical_dtype(s):
            s = s.cat.add_categories([""]).fillna("")
        else:
            s = s.astype("string").fillna("")

        s = (
            s.astype(str)
             .str.strip()
        )

        # remover ,0 / .0 no final
        s = s.str.replace(r'(,0|\.0)$', '', regex=True)

        # remover acentos (vetorizado)
        s = remover_acentos_series(s)

        # caixa alta
        s = s.str.upper()

        # normalizações
        s = (
            s.str.replace(r'\s+', ' ', regex=True)
             .str.replace(r'[;,&:./\-]', '', regex=True)
        )

        if separator:
            s = s.str.replace(separator, " ", regex=False)

        # tratar NAN / N/A
        s = s.where(~s.isin(["NAN", "N/A"]), "")

        df[col] = s
    return df

def encontra_municipio_por_cep(cep:str, dict_ceps:dict) -> str:
    """
    Recebe um CEP (string), consulta o ViaCEP e devolve o município (localidade).
    
    Exemplo:
        >>> get_municipio_por_cep("01001-000")
        'São Paulo'
    """
    # Mantém apenas os dígitos
    cep = "".join(filter(str.isdigit, cep))
    
    if cep in list(dict_ceps.keys()):
        return dict_ceps[cep]

    if len(cep) != 8:
        return 'NAO ENCONTRADO'
    
    url = f"https://viacep.com.br/ws/{cep}/json/"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()            # dispara para códigos HTTP ≠ 200
        data = resp.json()
    except requests.RequestException as e:
        return "NAO ENCONTRADO"
    
    if data.get("erro"):
        return "NAO ENCONTRADO"
    
    dict_ceps[cep] = data["localidade"]
    return data["localidade"]  

def transforma_data_em_str(data: str):
    if not data or not isinstance(data, str):
        return None

    # Remove espaços e pega só a parte da data (desconsidera a hora se houver)
    data = data.strip().split(" ")[0]
    
    # Tenta identificar formatos possíveis
    formatos = []
    if "-" in data:
        # Pode ser 'YYYY-MM-DD' ou 'YY-MM-DD'
        if re.match(r"\d{4}-\d{2}-\d{2}$", data):
            formatos.append("%Y-%m-%d")
        if re.match(r"\d{2}-\d{2}-\d{2}$", data):
            formatos.append("%y-%m-%d")
    elif "/" in data:
        # Pode ser 'DD/MM/YYYY' ou 'DD/MM/YY'
        if re.match(r"\d{2}/\d{2}/\d{4}$", data):
            formatos.append("%d/%m/%Y")
        if re.match(r"\d{2}/\d{2}/\d{2}$", data):
            formatos.append("%d/%m/%y")

    for formato in formatos:
        try:
            dt = datetime.strptime(data, formato)
            # Se quiser adicionar timezone, descomente a linha abaixo:
            # dt = dt.replace(tzinfo=tz_sp)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            continue

    return None

def gera_e_atualiza_quarentena(raiz, db):
      # mantém o fuso correto
    relatorio = ""
    erros = []
    try:
        PASTA_QUARENTENA = os.path.join(raiz, f"arquivos_quarentena")
        os.makedirs(PASTA_QUARENTENA, exist_ok=True)
        PASTA_TELS_NEXT = os.path.join(raiz, "arquivos_tels_next")
        os.makedirs(PASTA_TELS_NEXT, exist_ok=True)
        
        ARQUIVO_QUARENTENA = os.path.join(raiz, "arquivos_quarentena/quarentena.csv")
        if not os.path.exists(ARQUIVO_QUARENTENA):
            pd.DataFrame([],columns=["Data/Hora","Quarentena","Telefone"]).to_csv(ARQUIVO_QUARENTENA, sep=";", index=False)
        analisa_quarentena(raiz)

        df_quarentena = pd.read_csv(ARQUIVO_QUARENTENA, sep=";", dtype=str)

        

        for file in list(sorted(os.listdir(PASTA_QUARENTENA))):
            filename = os.path.join(PASTA_QUARENTENA, file)
            extensao = os.path.splitext(filename)[1].lower()
            if filename != ARQUIVO_QUARENTENA:
                if extensao in [".csv", ".txt"]:
                    df_perfilamento = pd.read_csv(filename, sep=_detectar_sep_csv(filename), dtype=str, encoding=_detectar_encoding_csv(filename),on_bad_lines="skip")
                else:#[".xls", '.xlsx', ".xlsb"]
                    df_perfilamento = pd.read_excel(
                        filename,
                        dtype=str,
                    )

                df_perfilamento.columns = df_perfilamento.columns.str.lower()


                relatorio += f"Analisando arquivo {file}\n"


                # mantém só os perfis mapeados
                
                df_perfilamento = df_perfilamento[["telefone", "tempo quarentena em dias", "data"]]
                # aplica a regra de quarentena
                df_perfilamento["Quarentena"] = df_perfilamento["tempo quarentena em dias"]

                # limpa telefone
                df_perfilamento["Telefone"] = df_perfilamento["telefone"].apply(clean_phone_number)
                df_perfilamento = df_perfilamento[df_perfilamento["Telefone"] != ""]

                relatorio += f"Total de  {len(df_perfilamento['Telefone'].unique().tolist())} telefones válidos encontrados no arquivo\n"
                
                df_perfilamento = df_perfilamento[~df_perfilamento["Telefone"].isin(df_quarentena["Telefone"])]
                
                relatorio += f"Dos quais, {len(df_perfilamento['Telefone'].unique().tolist())} São telefones novos\n"
                # ---------- NOVO: carimbo de data/hora ----------
                
                df_perfilamento["Data/Hora"] = df_perfilamento["data"].apply(transforma_data_em_str)         

                # coloca as colunas na ordem desejada
                df_perfilamento = df_perfilamento[["Data/Hora", "Quarentena", "Telefone"]]

                # acumula no dataframe final
                relatorio += f"Antes de cadastrar os novos, base de quarentena possuía {len(df_quarentena['Telefone'].unique().tolist())} telefones\n"
                df_quarentena = pd.concat([df_quarentena, df_perfilamento], ignore_index=True)
                os.remove(filename)

        #para telsnext
        for file in list(sorted(os.listdir(PASTA_TELS_NEXT))):
            filename = os.path.join(PASTA_TELS_NEXT, file)
            extensao = os.path.splitext(filename)[1].lower()
            if filename != ARQUIVO_QUARENTENA:
                if extensao in [".csv", ".txt"]:
                    df_perfilamento = pd.read_csv(filename, sep=_detectar_sep_csv(filename), dtype=str, encoding=_detectar_encoding_csv(filename),on_bad_lines="skip")
                else:#[".xls", '.xlsx', ".xlsb"]
                    df_perfilamento = pd.read_excel(
                        filename,
                        dtype=str,
                    )

                df_perfilamento.columns = df_perfilamento.columns.str.lower()



                # mantém só os perfis mapeados
                

                # aplica a regra de quarentena
                df_perfilamento["Quarentena"] = "160"

                # limpa telefone
                df_perfilamento["Telefone"] = df_perfilamento["numero_chamado"].apply(clean_phone_number)
                df_perfilamento = df_perfilamento[df_perfilamento["Telefone"] != ""]

                # ---------- NOVO: carimbo de data/hora ----------
                agora = datetime.now(tz_sp).strftime("%d/%m/%Y %H:%M:%S")
                df_perfilamento["Data/Hora"] = agora         

                # coloca as colunas na ordem desejada
                df_perfilamento = df_perfilamento[["Data/Hora", "Quarentena", "Telefone"]]

                # acumula no dataframe final
                df_quarentena = pd.concat([df_quarentena, df_perfilamento], ignore_index=True)

                os.remove(filename)

        df_quarentena.drop_duplicates(subset=["Telefone",], keep="last", inplace=True)
        df_quarentena.to_csv(ARQUIVO_QUARENTENA, sep=";", index=False)

        telefones = analisa_quarentena(raiz)
        relatorio += f"No fim, base de quarentena ficou com {len(telefones)} telefones após remover aqueles que passaram da data de quarentena."
        
        salva_dado("Quantidade de Telefones em Quarentena", len(telefones))
        # blacks = db.objects.filter()[0]
        # blacks.total_dados = len(telefones)
        # blacks.save()
        

        # salva_log("Finalizando Atualização da blacklist", "oi")

        return relatorio, erros
    except Exception as e:
        erros.append(traceback.format_exc())
        return relatorio, erros

def gera_e_atualiza_enriquecimento():
    relatorio = ""
    erros = []

    try:
        PASTA = os.path.join(os.getcwd(), "media/arquivos_enriquecimento")
        CSV_ATUAL = os.path.join(PASTA, "enriquecimento.csv")

        # 1) DataFrame existente ------------------------------------------------
        if os.path.exists(CSV_ATUAL):
            df_atual = pd.read_csv(CSV_ATUAL, sep=_detectar_sep_csv(CSV_ATUAL), dtype=str, encoding="latin-1").fillna("")
        else:
            df_atual = pd.DataFrame(columns=colunas_padrao)

        partes_long = [_phones_to_long(df_atual)]       # começamos com o antigo


        telefones_enr = []
        for i in range(1, 21):
            telefones_enr += df_atual[f"Telefone_{i}"].unique().tolist()

        total_telefones_enr = len(list(set(telefones_enr)))
        relatorio += f"Atualmente, a base da CredLink possui {len(df_atual['DOCUMENTO'].unique().tolist())} empresas e {total_telefones_enr} telefones.\n"
        
        # 2) Lê cada arquivo novo ----------------------------------------------
        for file in sorted(os.listdir(PASTA)):
            filename = os.path.join(PASTA, file)
            if filename == CSV_ATUAL:
                continue

            ext = os.path.splitext(filename)[1].lower()
            try:
                if ext in (".csv", ".txt"):
                    df = pd.read_csv(filename, sep=_detectar_sep_csv(filename), dtype=str, encoding="latin-1",on_bad_lines="skip")
                elif ext in (".xls", ".xlsx", ".xlsb"):
                    df = pd.read_excel(filename, dtype=str)
                else:
                    os.remove(filename)
                    relatorio += f"Arquivo {file} não considerado por não possuir formato inválido"
                    continue
            except Exception as e:
                os.remove(filename)
                relatorio += f"Arquivo {file} não considerado por ocorrer um erro desconhecido: {e}"
                continue

            # identifica colunas -----------------------------------------------
            doc_col = next((c for c in df.columns
                            if any(tok in c.lower() for tok in ("cpf", "cnpj"))), None)
            fone_cols = [c for c in df.columns
                           if any(tok in c.lower() for tok in ("tel", "celular", "fixo"))]
            
            relatorio += f"Foram encontradas as seguintes colunas de telefone que serão consideradas: {fone_cols}\n"
            if not (doc_col and fone_cols):
                os.remove(filename)

                relatorio += f"Arquivo {file} não considerado por não possuir colunas de telefone ou documento"
                continue

            df = df[[doc_col] + fone_cols].rename(columns={doc_col: "DOCUMENTO"}).fillna("")
            # padroniza para 20 colunas wide antes de melt (não custa nada)
            for i, col in enumerate(fone_cols, 1):
                df.rename(columns={col: f"Telefone_{i}"}, inplace=True)
            for i in range(len(fone_cols) + 1, 21):
                df[f"Telefone_{i}"] = ""

            telefones_arquivo = []
            for i in range(1, 21):
                telefones_arquivo += df[f"Telefone_{i}"].unique().tolist()

            total_telefones_arquivo = len(list(set(telefones_arquivo)))
            relatorio += f"Foram encontradas {len(df['DOCUMENTO'].unique().tolist())} empresas e {total_telefones_arquivo} telefones no arquivo {file}\n"

            partes_long.append(_phones_to_long(df))

            os.remove(filename)                       # opcional: descarta arquivo

            # 3) Concatena tudo no formato long ------------------------------------
            long_all = pd.concat(partes_long, ignore_index=True)

            # 3a) Remove duplicatas globais (primeira ocorrência vence)
            long_all = long_all.drop_duplicates(subset="TELEFONE", keep="first")

            # 3b) Mantém no máximo 20 números por documento
            long_all["idx"] = long_all.groupby("DOCUMENTO").cumcount()
            long_all = long_all[long_all["idx"] < 20]

            # 4) Volta para wide ----------------------------------------------------
            wide = (long_all
                    .pivot(index="DOCUMENTO", columns="idx", values="TELEFONE")
                    .rename(columns=lambda i: f"Telefone_{i+1}")
                    .reset_index())

            # garante todas as 20 colunas
            for col in telefone_cols:
                if col not in wide.columns:
                    wide[col] = ""

            # 5) Ordena, preenche vazios e grava -----------------------------------
            wide = wide[colunas_padrao].fillna("").astype(str)
            wide.to_csv(CSV_ATUAL, sep=_detectar_sep_csv(CSV_ATUAL), index=False)

            telefones_arquivo = []
            for i in range(1, 21):
                telefones_arquivo += wide[f"Telefone_{i}"].unique().tolist()

            total_telefones_arquivo = len(list(set(telefones_arquivo)))
            relatorio += f"Após cadastrar o arquivo, Base de Enriquecimento ficou com um total de {len(wide['DOCUMENTO'].unique().tolist())} empresas e {total_telefones_arquivo} telefones cadastrados.\n"

            salva_dado("Total de telefones na base de enriquecimento", total_telefones_arquivo)
        
        for file in sorted(os.listdir(PASTA)):
            filename = os.path.join(PASTA, file)
            if filename != CSV_ATUAL:
                os.remove(filename)
        return relatorio, erros
    
        

    except Exception:
        erros.append(traceback.format_exc())
        return relatorio, erros     
    
def processar_arquivo_individual(caminho_completo: str, campos_obrigatorios: List[str]) -> pd.DataFrame:
    extensao = os.path.splitext(caminho_completo)[1].lower()
    
    try:
        if extensao in [".csv", ".txt"]:
            # Nota: Certifique-se que _detectar_encoding_csv esteja acessível ou importada
            enc = _detectar_encoding_csv(caminho_completo) 
            df = pd.read_csv(caminho_completo, sep=";", dtype=str, encoding=enc, on_bad_lines="skip")
        elif extensao in [".xlsx", ".xls", ".xlsb"]:
            df = pd.read_excel(caminho_completo, dtype=str)
        else:
            return pd.DataFrame()

        # Rename unificado
        mapeamento = {
            "ï»¿CNPJ": "CNPJ", "APROVACAO_CREDITO": "APROVADO/NEGADO",
            "LETRA_MOTIVO_NEGATIVA": "LETRAS_STATUS", "CREDITO_PREAPROVADO": "APROVADO/NEGADO",
            "LETRA_MOTIVO_NEGACAO": "LETRAS_STATUS", "cnpj": "CNPJ",
            "APROVADO": "APROVADO/NEGADO", "COD_MAILING": "LETRAS_STATUS"
        }
        df.rename(columns=mapeamento, inplace=True)

        # Garantir campos e limpar CNPJ de forma vetorizada (Rápido)
        df = df[df.columns.intersection(campos_obrigatorios)].copy()
        if "CNPJ" in df.columns:
            df["CNPJ"] = df["CNPJ"].str.replace(r'\D', '', regex=True).str.zfill(14)
            df.dropna(subset=["CNPJ"], inplace=True)
            df.drop_duplicates(subset=["CNPJ"], inplace=True)
            
        return df
    except Exception:
        return pd.DataFrame()

def gera_e_atualiza_dados_credito_turbo(raiz="media"):
    relatorio = ""
    erros = []
    CAMPOS = ["CNPJ", "APROVADO/NEGADO", "LETRAS_STATUS"]
    
    try:
        PASTA_CREDITO = os.path.join(raiz, "arquivos_credito")
        os.makedirs(PASTA_CREDITO, exist_ok=True)
        ARQUIVO_MASTER = os.path.join(PASTA_CREDITO, "credito.csv")

        # 1. Carregar base atual
        if os.path.exists(ARQUIVO_MASTER):
            df_master = pd.read_csv(ARQUIVO_MASTER, sep=";", dtype=str)
        else:
            df_master = pd.DataFrame(columns=CAMPOS)

        relatorio += f"Base atual: {len(df_master)} CNPJs.\n"

        # 2. Identificar arquivos para processar
        arquivos_para_processar = [
            os.path.join(PASTA_CREDITO, f) for f in os.listdir(PASTA_CREDITO) 
            if os.path.join(PASTA_CREDITO, f) != ARQUIVO_MASTER and os.path.isfile(os.path.join(PASTA_CREDITO, f))
        ]

        if not arquivos_para_processar:
            return "Nenhum arquivo novo.", []

        # 3. Processamento Paralelo (Uso intensivo de Cores/RAM)
        # O max_workers padrão usa todos os núcleos lógicos da máquina
        lista_dfs_novos = []
        with ProcessPoolExecutor() as executor:
            resultados = list(executor.map(processar_arquivo_individual, arquivos_para_processar, [CAMPOS]*len(arquivos_para_processar)))
            lista_dfs_novos = [d for d in resultados if not d.empty]

        # 4. Consolidação Final (Único ponto de escrita pesada em memória)
        if lista_dfs_novos:
            df_novos_total = pd.concat(lista_dfs_novos, ignore_index=True)
            
            # Ordem cronológica: Master antigo primeiro, novos depois. 
            # O drop_duplicates com keep='last' garante que o dado novo vença.
            df_final = pd.concat([df_master, df_novos_total], ignore_index=True)
            df_final.drop_duplicates(subset=["CNPJ"], keep="last", inplace=True)
            
            # Salvar
            df_final.to_csv(ARQUIVO_MASTER, sep=";", index=False)
            
            # Limpeza física dos arquivos processados
            for f in arquivos_para_processar:
                try: os.remove(f)
                except: pass

            relatorio += f"Processamento concluído. Base final: {len(df_final)} CNPJs.\n"
            salva_dado(titulo="Total de empresas com crédito informado", quantidade=len(df_final), sistema="geral")
        
        return relatorio, erros

    except Exception as e:
        erros.append(traceback.format_exc())
        return relatorio, erros

def filtra_mailing(df:pd.DataFrame) -> pd.DataFrame:
    if len(df.index) == 0:
        return df
    ini = time.time()
    telefones_blacklist = ler_arquivos_e_coletar_telefones()
    
    ini = time.time()
    quarentena = analisa_quarentena("media")

    blacklist_set = set(telefones_blacklist + quarentena)

    tipos_colunas_telefone = [
        ["TEL1", "TEL2", "TEL3"],
        [f"Telefone_{i}" for i in range(1, 21)]
    ]

    for colunas_telefone in tipos_colunas_telefone:
        ini = time.time()

        for col in colunas_telefone:
            df[col] = df[col].where(
                ~df[col].isin(blacklist_set), 
                ""
            )

        phones_matrix = df[colunas_telefone].values
        filtered_phones = []

        for row in phones_matrix:
            valid_phones = [str(phone) for phone in list(set(row)) if str(phone).strip() and phone not in blacklist_set]
            blacklist_set.update(set(valid_phones))

            to_add = valid_phones + [''] * (len(colunas_telefone)- len(valid_phones))
            filtered_phones.append(to_add)
        df[colunas_telefone] = filtered_phones
        for c in colunas_telefone:
            df[c] = df[c].apply(clean_phone_number)
    return df


def filtra_arquivos(raiz, pasta_arquivos_para_filtrar, pasta_usuario) -> str:
    relatorio = ""
    erros = []
    try:
        telefones_blacklist = ler_arquivos_e_coletar_telefones()
        quarentena = analisa_quarentena(raiz)
        blacklist_set = set(telefones_blacklist + quarentena)
        relatorio += f"Total de {len(blacklist_set)} telefones na blacklist e quarentena. \n"

        for arq in os.listdir(pasta_arquivos_para_filtrar):
            formato_com_ddd = False
            file = os.path.join(pasta_arquivos_para_filtrar, arq)
            file_sem_contato = os.path.join(pasta_arquivos_para_filtrar, "sem_contato"+arq)
            extensao = os.path.splitext(file)[1].lower()
            sep = _detectar_sep_csv(file)
            encod = _detectar_encoding_csv(file)

            df = pd.DataFrame([])
            if extensao in [".csv", ".txt"]:
                df = pd.read_csv(file, sep=sep, dtype=str, encoding=encod,on_bad_lines="skip")
            else:#[".xls", '.xlsx', ".xlsb"]
                df = pd.read_excel(
                    file,
                    dtype=str,
                )

            df = padronizacao(df, separator=sep)
            colunas_telefone = []

            for col in df.columns:
                if "ddd" in str(col).lower():
                    formato_com_ddd = True
                    relatorio +=  f"Indentificou o formato como DDD + TEL. \n"

                    break
            
            if formato_com_ddd:
                for i in range(1,7):
                    df[f"telefone_{i}"] = df[f"DDD{i}"].astype(str) + df[f"TEL{i}"].astype(str)

            
            
                for col in df.columns:
                    if "telefone_" in str(col).lower():
                        colunas_telefone.append(col)
                        df[col] = df[col].apply(lambda x: clean_phone_number(x))
            else:
                for col in df.columns:
                    if "tel" in str(col).lower():
                        colunas_telefone.append(col)
                        df[col] = df[col].apply(lambda x: clean_phone_number(x))
                
            total_antes = sum(df[col].nunique() for col in colunas_telefone)
            relatorio +=  f"Antes de filtrar, o arquivo {arq} possuía {total_antes} telefones. \n"
            

            for col in colunas_telefone:
                df[col] = df[col].where(
                    ~df[col].isin(blacklist_set), 
                    ""
                )

            for col in colunas_telefone:
                df[col] = df[col].mask(
                    df[col] == "NAO ENCONTRADO",
                    ""                                # ou pd.NA / np.nan
                )
                
            phones_matrix = df[colunas_telefone].values
            filtered_phones = []
            for row in phones_matrix:
                valid_phones = [str(phone) for phone in list(set(row)) if clean_phone_number(str(phone).strip()) and phone not in blacklist_set]
                blacklist_set.update(set(valid_phones))
                filtered_phones.append(valid_phones + [''] * (len(colunas_telefone)- len(valid_phones)))

            df[colunas_telefone] = filtered_phones

            # Log depois
            total_depois = sum(df[col].nunique() for col in colunas_telefone)
            relatorio +=  f"Após filtrar blacklist e quarentena, arquivo {arq} possui {total_depois} telefones\n"


            coluna_telefone = colunas_telefone[0]
            if formato_com_ddd:
                for i in range(1,9):
                    if i < 7:
                        df[f"DDD{i}"] = df[f"telefone_{i}"].apply(lambda x: str(x)[:2] if str(x).strip() != "" else "")
                        df[f"TEL{i}"] = df[f"telefone_{i}"].apply(lambda x: str(x)[2:] if str(x).strip() != "" else "")
                        df = df.drop(f"telefone_{i}", axis=1)
                    else:
                        df = df.drop(f"TEL{i}", axis=1)
                        df = df.drop(f"DDD{i}", axis=1)
                coluna_telefone = "TEL1"

            df.replace("NAO ENCONTRADO", "", inplace=True)
            df.replace(";", ",", inplace=True)
                
            df_vazios = df[df[coluna_telefone] == ""]
            df = df[df[coluna_telefone] != ""]

            

            if extensao in [".csv", ".txt"]:
                df.to_csv(file,sep=sep, index=False)
            else:#[".xls", '.xlsx', ".xlsb"]         
                df.to_excel(file, index=False)

            #SALVANDO SEM CONTATOS
            if extensao in [".csv", ".txt"]:
                df_vazios.to_csv(file_sem_contato,sep=sep, index=False)
            else:#[".xls", '.xlsx', ".xlsb"]         
                df_vazios.to_excel(file_sem_contato, index=False)

            relatorio +=  f"Arquivo {arq} filtrado com sucesso"
        
        zip_folder(pasta_arquivos_para_filtrar, os.path.join(pasta_usuario,"arquivos_filtragem.zip"))
        for arq in os.listdir(pasta_arquivos_para_filtrar):
            file = os.path.join(pasta_arquivos_para_filtrar, arq)
            os.remove(file)
        return relatorio,erros
    except Exception as e:
        erros.append(traceback.format_exc())
        return relatorio, erros

def complementa_arquivos(pasta_usuario, pasta_destino) -> str:
    relatorio = ""
    erros = []
    try:
        PASTA_ARQUIVOS_COMPLEMENTAR = pasta_destino
        os.makedirs(PASTA_ARQUIVOS_COMPLEMENTAR, exist_ok=True)

        enriquecimento = os.path.join(os.getcwd(), 'media/arquivos_enriquecimento/enriquecimento.csv')
        df_enriquecimento = pd.read_csv(enriquecimento, sep=";")
        df_enriquecimento["DOCUMENTO"] = df_enriquecimento["DOCUMENTO"].astype("string")
        df_enriquecimento["DOCUMENTO"] = df_enriquecimento["DOCUMENTO"].apply(lambda x: re.sub(r'\D', '', x))
        df_enriquecimento["DOCUMENTO"] = df_enriquecimento["DOCUMENTO"].astype(str).str.zfill(14)

        for arq in os.listdir(PASTA_ARQUIVOS_COMPLEMENTAR):
            file = os.path.join(PASTA_ARQUIVOS_COMPLEMENTAR, arq)
            extensao = os.path.splitext(file)[1].lower()
            sep = _detectar_sep_csv(file)
            

            df = pd.DataFrame([])
            if extensao in [".csv", ".txt"]:
                encoding = _detectar_encoding_csv(file)
                df = pd.read_csv(file, sep=sep, dtype=str, encoding=encoding,on_bad_lines="skip")
            else:#[".xls", '.xlsx', ".xlsb"]
                df = pd.read_excel(
                    file,
                    dtype=str,
                )

            df = padronizacao(df, separator=sep)
            df.rename(columns={
                "CNPJ": "cnpj",
            },inplace=True)
            df["cnpj"] = df["cnpj"].apply(lambda x: re.sub(r'\D', '', x))
            permitidos = df["cnpj"].unique().tolist()
            df_enriquecimento = df_enriquecimento[df_enriquecimento["DOCUMENTO"].isin(permitidos)]
            
            df_final = pd.merge(df, df_enriquecimento,how="left", left_on="cnpj", right_on="DOCUMENTO").fillna("")
            
            df_final.drop(columns=["DOCUMENTO"], axis=1, inplace=True)
            
            print(f"Tamanho final do arquivo: {len(df.index)}")

            if extensao in [".csv", ".txt"]:
                df_final.to_csv(file,sep=sep, index=False)
            else:#[".xls", '.xlsx', ".xlsb"]         
                df_final.to_excel(file, index=False)

            del df
            del df_final

            relatorio +=  f"Arquivo {file} complementado com sucesso"

        del df_enriquecimento

        zip_folder(PASTA_ARQUIVOS_COMPLEMENTAR, os.path.join(pasta_usuario, "arquivos_complementar.zip"))
        # for arq in os.listdir(PASTA_ARQUIVOS_COMPLEMENTAR):
        #     file = os.path.join(PASTA_ARQUIVOS_COMPLEMENTAR, arq)
        #     os.remove(file)
        return relatorio,erros
    
    except Exception as e:
        erros.append(traceback.format_exc())
        return relatorio, erros

def complementa_cnpj(raiz) -> str:
    relatorio = ""
    erros = []
    try:
        pasta_arquivos_para_complementar = os.path.join(os.getcwd(), "media", "arquivos_cnpj")
        for arq in os.listdir(pasta_arquivos_para_complementar):
            file = os.path.join(pasta_arquivos_para_complementar, arq)
            extensao = os.path.splitext(file)[1].lower()
            sep = _detectar_sep_csv(file)
            

            df_para_complementar = pd.DataFrame([])
            if extensao in [".csv", ".txt"]:
                encoding = _detectar_encoding_csv(file)
                df_para_complementar = pd.read_csv(file, sep=sep, dtype=str, encoding=encoding,on_bad_lines="skip")
            else:#[".xls", '.xlsx', ".xlsb"]
                df_para_complementar = pd.read_excel(
                    file,
                    dtype=str,
                )

            if "cnpj" not in [str(c).lower() for c in df_para_complementar.columns.to_list()]:
                erros.append(f"Falha ao ler arquivos para enriquecer pois o arquivo {arq} não possui uma coluna 'cnpj'")
                return relatorio, erros

            df_para_complementar["cnpj"] = df_para_complementar["cnpj"].astype("string")
            df_para_complementar["cnpj"] = df_para_complementar["cnpj"].apply(lambda x: re.sub(r'\D', '', x))
            df_para_complementar["cnpj"] = df_para_complementar["cnpj"].astype(str).str.zfill(14)
            quantidade_dados_inicial = len(df_para_complementar.index)
        
            colunas = df_para_complementar.columns.to_list()

            


            pastas_procurar = [
                os.path.join(os.getcwd(), "media", "viabilidades_credito_enriquecido"),
                os.path.join(os.getcwd(), "media", "viabilidades_credito_nao_informado_enriquecido"),
                os.path.join(os.getcwd(), "media", "viabilidades_credito_pre_negado_enriquecido"),
                os.path.join(os.getcwd(), "media_janeiro_2026", "viabilidades_credito_enriquecido"),
                os.path.join(os.getcwd(), "media_janeiro_2026", "viabilidades_credito_nao_informado_enriquecido"),
                os.path.join(os.getcwd(), "media_janeiro_2026", "viabilidades_credito_pre_negado_enriquecido"),
            ]
            dfs = []
            for pasta in pastas_procurar:
                arquivos_base = os.listdir(pasta)
                for arq_base in arquivos_base:
                    cnpjs_restantes = df_para_complementar["cnpj"].to_list()

                    file_base = os.path.join(pasta, arq_base)
                    df = pd.read_csv(file_base, sep=";", dtype={"data_inicio_atividades":"string", "natureza_juridica": "category", "descricaonj":"category", "cnae_fiscal":"string", "cnae_fiscal_secundaria":"string", "descricaocf":"category", "cnpj":"string", "razao_social":"string", "nome_fantasia": "string", "matriz_filial":"category", "decisor":"string", "situacao_cadastral":"category", "correio_eletronico":"string", "logradouro":"string", "num_fachada":"string", "complemento1":"string", "bairro":"string", "cep":"string", "municipio":"category","uf":"category", "CPF":"string", "MEINAOMEI": "category", "TEL1":"string", "TEL2":"string", "TEL3":"string"})
                    df["cnpj"] = df["cnpj"].astype("string")
                    df["cnpj"] = df["cnpj"].apply(lambda x: re.sub(r'\D', '', x))
                    df["cnpj"] = df["cnpj"].astype(str).str.zfill(14)

                    df = df[df["cnpj"].isin(cnpjs_restantes)]
                    cnpjs_encontrados = df["cnpj"].to_list()
                    if len(df.index)>0:
                        print(f"Dado encontrado no arquivo {file_base}")
                        df_unido = pd.merge(df_para_complementar, df, on="cnpj", how="inner")
                        df_para_complementar = df_para_complementar[~df_para_complementar["cnpj"].isin(cnpjs_encontrados)]

                        dfs.append(df_unido)

            df_unificado = pd.concat(dfs)
            df_unificado["cnpj"] = df_unificado["cnpj"].astype("string")
            df_unificado["cnpj"] = df_unificado["cnpj"].apply(lambda x: re.sub(r'\D', '', x))
            df_unificado["cnpj"] = df_unificado["cnpj"].astype(str).str.zfill(14)
            df_unificado.drop_duplicates(subset=["cnpj"], inplace=True)
            df_unificado.dropna(subset=["cnpj"], inplace=True)
            
            if len(df_para_complementar.index) > 0:
                for col in colunas:
                    df_para_complementar[col] = ""

                df_unificado = pd.concat([df_unificado, df_para_complementar])

            quantidade_final = len(df_unificado.index)
            if quantidade_dados_inicial != quantidade_final:
                erros.append(f"Falha ao finalizar unificação dos dados. Quantidade Inicial {quantidade_dados_inicial} X {quantidade_final} Quantidade unificada estão diferentes no arquivo {arq}")
                return relatorio, erros
            
            if extensao in [".csv", ".txt"]:
                df_unificado.to_csv(file,sep=sep, index=False)
            else:#[".xls", '.xlsx', ".xlsb"]         
                df_unificado.to_excel(file, index=False)

            relatorio += f"Tratamento e unificação do arquivo {arq} bem sucedida!"
            print(f"Tratamento e unificação do arquivo {arq} bem sucedida!")

        zip_folder(os.path.join(os.getcwd(), "media/arquivos_cnpj"), "media/arquivos_cnpj.zip")
        for arq in os.listdir(pasta_arquivos_para_complementar):
            file = os.path.join(pasta_arquivos_para_complementar, arq)
            os.remove(file)
            print(f"Arquivo {file} removido!")
        print("Retornando o relatório")
        return relatorio,erros
    
    except Exception as e:
        erros.append(traceback.format_exc())
        return relatorio, erros

def zip_folder(src_dir, dst_zip):
    """
    Compacta uma pasta inteira em um arquivo ZIP.
    
    Parâmetros:
    src_dir (str): Caminho da pasta a ser compactada
    dst_zip (str): Caminho do arquivo ZIP de destino
    """
    with zipfile.ZipFile(dst_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        base_dir = os.path.basename(os.path.normpath(src_dir))
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.join(base_dir, os.path.relpath(file_path, src_dir))
                zipf.write(file_path, arcname)

def fecha_conexoes(func):
    def wrapper(*args, **kwargs):
        connection.close()
        result = func(*args, **kwargs)
        connection.close()
        return result
    return wrapper

def verifica_arquivo(request, arquivo_original, caminho: str, nome_pasta: str, sistema:str) -> list:
    try:
        # ── 1. Nunca tente ler um arquivo vazio ────────────────────────────────
        if os.path.getsize(caminho) == 0:
            return True, f"O arquivo {arquivo_original} está vazio."

        dicionario_colunas_sistema = {
            "arquivos_enriquecimento": verifica_base_enriquecimento,
            "arquivos_quarentena": verifica_base_perfilamento,
            "arquivos_dfv": verifica_base_dfv,
            "arquivos_credito": verifica_base_credito,
            "arquivos_filtragem": verifica_arquivos_filtragem,
            "arquivos_complementar": verifica_arquivos_complementar,
            "arquivos_blacklist": verifica_arquivos_blacklist,
        }

        extensao = os.path.splitext(caminho)[1].lower()
        nrows = 300 if nome_pasta != "arquivos_credito" else 50_000
        # ── 2. Carrega só as primeiras linhas, conforme a extensão ─────────────
        if extensao in (".csv", ".txt"):
            # pegamos só o primeiro chunk (até 300 linhas) para validar
            chunks = pd.read_csv(
                caminho,
                sep=_detectar_sep_csv(caminho),
                dtype=str,
                chunksize=nrows,
                encoding=_detectar_encoding_csv(caminho),
            )
            df = next(chunks)  # primeiro pedaço
        elif extensao in (".xls", ".xlsx", ".xlsb"):
            df = pd.read_excel(caminho, dtype=str, nrows=nrows)
        else:
            os.remove(caminho)
            return False, f"Arquivo >{arquivo_original}< possui extensão não suportada: {extensao}"

        # ── 3. Garante que o DataFrame não está vazio ─────────────────────────
        if df.empty:
            os.remove(caminho)
            return False, f"O arquivo {arquivo_original} não contém dados."
        
        if nome_pasta not in list(dicionario_colunas_sistema.keys()):
            print("NOME DA PASTA", nome_pasta)
            return True, "Arquivos Carregados com sucesso!"
        
        # ── 4. Encaminha para a função de validação/colunas correspondente ────
        return dicionario_colunas_sistema[nome_pasta](
            request, extensao, arquivo_original, caminho, df, sistema
        )

    except Exception as e:
        
        return False, f"Erro desconhecido ao tentar cadastrar '{arquivo_original}': {e}"

def verifica_base_enriquecimento(request, extensao, arquivo_original, caminho_final, df:pd.DataFrame,sistema:str) -> list:

    #validando extensão do arquivo
    extensoes_permitidas = [".txt", ".csv",".xlsx", ".xls",".xlsb"]
    if extensao not in extensoes_permitidas:
        os.remove(caminho_final)
        return False,f"Arquivo {arquivo_original.name} Não considerado por ter extensão inválida! Extensões válidas: {extensoes_permitidas}"
    
    doc_col = next((c for c in df.columns if any(tok in c.lower() for tok in ("cpf", "cnpj"))), None)
    if not doc_col:
        os.remove(caminho_final)
        return False, f"Arquivo {arquivo_original.name} Não considerado por não possuir ao menos uma coluna 'cnpj' ou 'cpf'"

    fone_cols = [c for c in df.columns if any(tok in c.lower() for tok in ("tel", "celular", "fixo"))]
    if not fone_cols:
        os.remove(caminho_final)
        return False, f"Arquivo {arquivo_original.name} Não considerado por não possuir ao menos uma coluna com termos de telefone, como: 'tel', 'celular' ou 'fixo'"
    
    
    return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"

def verifica_base_perfilamento(request, extensao, arquivo_original, caminho_final, df:pd.DataFrame, sistema:str) -> list:
    
    col_telefone = "data" in [col.lower() for col in df.columns.to_list()]
    if not col_telefone:
        os.remove(caminho_final)
        return False, f"Arquivo {arquivo_original.name} Não considerado por não possuir coluna 'data'"
    
    col_telefone = "telefone" in [col.lower() for col in df.columns.to_list()]
    if not col_telefone:
        os.remove(caminho_final)
        return False, f"Arquivo {arquivo_original.name} Não considerado por não possuir coluna 'telefone'"
    
    col_tempo_quarentena = "tempo quarentena em dias" in [col.lower() for col in df.columns.to_list()]
    if not col_tempo_quarentena:
        os.remove(caminho_final)
        return False,f"Arquivo {arquivo_original.name} Não considerado por não possuir coluna 'tempo quarentena em dias'"

    df_5_linhas = df.head()
    df_5_linhas.columns = df_5_linhas.columns.str.lower()
    for index,row in df_5_linhas.iterrows():
        data = transforma_data_em_str(row["data"])
        if data == None:
            os.remove(caminho_final)
            return False,f"Arquivo {arquivo_original.name} Não considerado pois a coluna 'data' está em formato desconhecido"
        dias = row["tempo quarentena em dias"]
        if not str(dias).strip().isnumeric():
            return False,f"Arquivo {arquivo_original.name} Não considerado pois a coluna 'tempo quarentena em dias' não possui apenas números"

    return True,f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"

def verifica_base_dfv(request, extensao, arquivo_original,caminho_final, df:pd.DataFrame, sistema:str) -> list:
    print("verificando dfv")
    if sistema == "oi":
        COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
    elif sistema == "giga_mais":
        COLUNAS_DFV = ["TERRITÓRIO","CIDADE","ESTADO","LOGRADOURO","CEP","BAIRRO",]
    elif sistema == "janeiro_2026":
        COLUNAS_DFV = [ "ENDERECO", "FACHADA", "CEP", ]



    extensoes_permitidas = [".xlsx", ".xls",".xlsb"]
    if extensao not in extensoes_permitidas:
        os.remove(caminho_final)
        return False, f"Arquivo {arquivo_original.name} Não considerado por ter extensão inválida! Extensões válidas: {extensoes_permitidas}"
    

    colunas_df_padronizadas = [col.lower() for col in df.columns.to_list()]
    for coluna in COLUNAS_DFV:
        if coluna.lower() not in colunas_df_padronizadas:
            os.remove(caminho_final)
            return False,f"Arquivo {arquivo_original.name} Não considerado por não possuir as colunas necessárias. As colunas devem ser: {COLUNAS_DFV}"
    
    
    
    return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"
    
def verifica_base_credito(request, extensao, arquivo_original,caminho_final, df:pd.DataFrame,sistema:str) -> list:
    try:
        df.rename(columns={
            "ï»¿CNPJ": "CNPJ",
            "APROVACAO_CREDITO": "APROVADO/NEGADO",
            "LETRA_MOTIVO_NEGATIVA":"LETRAS_STATUS",
            "CREDITO_PREAPROVADO": "APROVADO/NEGADO",
            "LETRA_MOTIVO_NEGACAO": "LETRAS_STATUS",
            "cnpj": "CNPJ",
            "APROVADO":"APROVADO/NEGADO",
            "COD_MAILING": "LETRAS_STATUS"
        },inplace=True)

        colunas_df_padronizadas = df.columns.to_list()
        colunas_formato_credito = ["CNPJ","APROVADO/NEGADO","LETRAS_STATUS"]

        

        extensoes_permitidas = [".csv", ".txt", ".xlsx"]
        if extensao not in extensoes_permitidas:
            os.remove(caminho_final)
            return False, f"Arquivo {arquivo_original.name} Não considerado por ter extensão inválida! Extensões válidas: {extensoes_permitidas}"
        

        i = 0
        for coluna in colunas_formato_credito:
            if coluna not in colunas_df_padronizadas:
                os.remove(caminho_final)
                return False, f"Arquivo {arquivo_original.name} Não considerado por não possuir as colunas necessárias. As colunas devem ser: {colunas_formato_credito}, respectivamente. Foram encontradas: {colunas_df_padronizadas}"

            i+=1

        lista_status = df["APROVADO/NEGADO"].unique().tolist()
        if "N" not in lista_status or "S" not in lista_status:
            os.remove(caminho_final)
            return False, f"Arquivo {arquivo_original.name} Não considerado pois a coluna de status deve possuir os termos 'S' para os aprovados e 'N' para os negados." 

        
        
        return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"

    except Exception as e:
        return False, f"Verificação no arquivo {arquivo_original.name} falhou, pois: {e}"

def verifica_arquivos_filtragem(request, extensao, arquivo_original, caminho_final, df:pd.DataFrame,sistema:str) -> list:
    formato_com_ddd = False
    for col in df.columns:
        if "ddd" in str(col).lower():
            formato_com_ddd = True
            break

    # if sistema=="giga_mais":
    #     formato_com_ddd = False
    
    if formato_com_ddd:
        for i in range(1,7):
            if f"DDD{i}" not in df.columns or f"TEL{i}" not in df.columns:
                return False, f"Arquivo {arquivo_original.name} Foi passado no formato IPBOX, porém não possui as 6 colunas 'DDD' ou 'TEL' necessárias"
    else:
        tem_coluna_telefone = False
        for col in df.columns:
            if "telefone" in str(col).lower():
                tem_coluna_telefone=True

        if not tem_coluna_telefone:
            return False, f"Arquivo {arquivo_original.name} Foi passado no formato GERAL, porém não possui nenhuma coluna com 'telefone' no nome."

    return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"

def verifica_arquivos_blacklist(request, extensao, arquivo_original, caminho_final, df:pd.DataFrame,sistema:str) -> list:
    try:
        def processar_arquivo(caminho, tipo) -> list:
            telefones_arquivo = []
            try:
                extensao = os.path.splitext(caminho)[1].lower()
                
                # Ler arquivo conforme a extensão
                if extensao == '.csv':
                    with open(caminho, 'r') as f:
                        dialeto = csv.Sniffer().sniff(f.read(1024))
                    df = pd.read_csv(caminho, sep=dialeto.delimiter, dtype=str)
                elif extensao in ('.xls', '.xlsx', ".xlsb"):
                    df = pd.read_excel(caminho, dtype=str)
                elif extensao == '.txt':
                    df = pd.read_csv(caminho, sep=';', dtype=str)
                else:
                    return

                # Padronizar nomes de colunas
                df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

                # Identificar colunas com telefone
                if tipo == "blacklist":
                    colunas_telefone = [col for col in df.columns if 'telefone' in col or "ddd" in col]
                else:
                    colunas_telefone = [col for col in df.columns if 'numero' in col or 'telefone' in col]
    
                # Adicionar números ao conjunto
                for coluna in colunas_telefone:
                    nums = df[coluna].dropna().astype(str).str.strip().to_list()
                    telefones_arquivo+=nums
                return telefones_arquivo

            except Exception as e: #tentando ler agora como lista em texto
                try:
                    with open(caminho, "r", encoding="latin-1") as arq:
                        numeros = [clean_phone_number(numb) for numb in arq.read().split("\n")]
                    return numeros
                except:
                    return []

        telefones_blacklist = []       
        # Processar todos os arquivos na pasta atual
        for arq in os.listdir(PASTA_ARQUIVOS_BLACKLIST):
            arquivo = os.path.join(PASTA_ARQUIVOS_BLACKLIST, arq)
            if os.path.isfile(arquivo):
                news_tels=processar_arquivo(arquivo, "blacklist")
                if type(news_tels) == list:
                    telefones_blacklist+=news_tels

        telefones_blacklist = list(set(telefones_blacklist))


        blacks = BaseBlackList.objects.filter()[0]
        blacks.total_dados = len(telefones_blacklist)
        blacks.save()



        return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso! Novo total de telefones na base de blacklist: {len(telefones_blacklist)}"

    except Exception as e:
        return True, f"Arquivo {arquivo_original.name} não considerado pois {traceback.format_exc()}"

def verifica_arquivos_complementar(request, extensao, arquivo_original, caminho_final, df:pd.DataFrame,sistema:str) -> list:
    if "cnpj" not in df.columns.tolist():
        return False, f"Arquivo {arquivo_original.name} não possui nenhuma coluna com 'cnpj' no nome."
    
    return True, f"Arquivo {arquivo_original.name} Cadastrado com sucesso!"

telefone_cols   = [f"Telefone_{i}" for i in range(1, 21)]
colunas_padrao  = ["DOCUMENTO"] + telefone_cols

def _phones_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Converte um DF no formato wide → long e limpa números vazios."""
    long = (df.melt(id_vars=["DOCUMENTO"],
                    value_vars=[c for c in df.columns if c.startswith("Telefone_")],
                    value_name="TELEFONE")
              .loc[lambda d: d["TELEFONE"].ne("")]
              .drop(columns="variable"))
    # normaliza telefone uma única vez, já vetorizado
    long["TELEFONE"] = long["TELEFONE"].map(lambda x: clean_phone_number(x, True))
    return long

def compacta_colunas(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
    """
    Reorganiza os valores das colunas indicadas para que todos os valores não nulos
    fiquem à esquerda e os vazios/NaN fiquem à direita.
    
    """
    # 1. Extraímos apenas as colunas necessárias como uma matriz NumPy
    arr = df[colunas].values
    
    # 2. Criamos uma máscara de valores "vazios" (NaN ou strings vazias)
    # pd.isna pega None/NaN, e o check de string lida com ""
    mask = pd.isna(arr) | (arr == "")
    
    # 3. Truque de ordenação: 
    # Usamos o argsort para mover os True (vazios) para o final de cada linha
    # O argsort é estável, então ele mantém a ordem original dos números
    idx = np.argsort(mask, axis=1)
    
    # 4. Reorganizamos a matriz usando os índices ordenados
    # np.take_along_axis é extremamente rápido para isso
    result = np.take_along_axis(arr, idx, axis=1)
    
    # 5. Atualizamos o DataFrame original
    df[colunas] = result
    return df

def get_dados_mailing(colunas_filtro:dict, campos_retorno:list=[], tipos_credito:list=[], formato_saida:str="padrao", conjunto_telefones:str="todos", tipos_telefone:str="todos", tipoMailing:str="ambos", filtro_telefone_blacklist:str="apenas_filtrados", pasta_dados:str="") -> pd.DataFrame:
    
    arquivos_para_ler = []
    colunas_ip_box = ["cnpj", "razao_social", "decisor", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "DDD1", "TEL1", "DDD2", "TEL2", "DDD3", "TEL3", "DDD4", "TEL4", "DDD5", "TEL5", "DDD6", "TEL6", "DDD7", "TEL7", "DDD8", "TEL8", ]
    colunas_vonix = ["TELEFONE1", "TELEFONE2", "TELEFONE3", "TELEFONE4", "TELEFONE5", "TELEFONE6", "TELEFONE7", "TELEFONE8", "CNPJ", "NOME", "DECISOR", "EMAIL", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO ALTO", "CIDADE", "UF", "ORIGEM",]
    for f in os.listdir(pasta_dados):
        df_path = os.path.join(pasta_dados, f)
        coletar = False
        for col in colunas_filtro["uf"]:
            if col in f and f.endswith(".csv"):
                if tipoMailing != "ambos":
                    if tipoMailing == "primario" and  "Secundaria" not in f:
                        coletar = True
                    if tipoMailing == "secundario" and  "Secundaria" in f:
                        coletar = True
                
                else:
                    coletar = True
        if coletar:
            arquivos_para_ler.append(df_path)


    def processa_arquivo(df_path:str,) -> pd.DataFrame:
        colunas_padrao = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3"]
        dtypes = {"data_inicio_atividades":"string", "natureza_juridica": "category", "descricaonj":"category", "cnae_fiscal":"string", "cnae_fiscal_secundaria":"string", "descricaocf":"category", "cnpj":"string", "razao_social":"string", "nome_fantasia": "string", "matriz_filial":"category", "decisor":"string", "situacao_cadastral":"category", "correio_eletronico":"string", "logradouro":"string", "num_fachada":"string", "complemento1":"string", "bairro":"string", "cep":"string", "municipio":"category","uf":"category", "CPF":"string", "MEINAOMEI": "category", "TEL1":"string", "TEL2":"string", "TEL3":"string"}
        colunas_enriquecimento = []
        
        for i in range(1,21):
            dtypes[f"Telefone_{i}"] = "string"
            colunas_enriquecimento.append(f"Telefone_{i}")
        dfs = []
        total = 0
        chunk_i = 0
        for chunk in pd.read_csv(df_path, sep=";", chunksize=1_000_000, dtype=dtypes):
            ini_time = time.time()
            total += len(chunk.index)
            mask = pd.Series(True, index=chunk.index)

            for coluna, valores in colunas_filtro.items():
                if coluna == "cnae_fiscal":
                    valores = [str(v).split(" - ")[0] for v in valores]
                
                if coluna == "termos_chave":
                    # Adicionando filtro por termo em nome_fantasia ou razao_social
                    if valores:  # Garante que não está vazio
                        if isinstance(valores, str):
                            termos = [valores]
                        else:
                            termos = valores

                        termos_mask = pd.Series(False, index=chunk.index)
                        for termo in termos:
                            termo = str(termo).strip()
                            if termo:
                                termos_mask |= (
                                    chunk["nome_fantasia"].astype(str).str.contains(termo, case=False, na=False) |
                                    chunk["razao_social"].astype(str).str.contains(termo, case=False, na=False)
                                )
                        mask &= termos_mask
                    continue  # pula para o próximo item do for
                
                if coluna == "MEINAOMEI":
                    chunk[coluna] = chunk[coluna].astype(str)
                    chunk[coluna].fillna("", inplace=True)
                    if valores == "N":
                        valores = ["", "N", "n"]
                    else:
                        valores = ["S", "s"]

                mask &= chunk[coluna].astype(str).isin(valores)
            
            chunk = chunk[mask]

            if conjunto_telefones == "apenas_original":
                chunk[colunas_enriquecimento] = ""

            # chunk["MEINAOMEI"] = chunk["MEINAOMEI"].apply(lambda x: "S" if str(x).strip().lower()=="s" else "N" )  
            if not chunk.empty:
                dfs.append(chunk)

            chunk_i+=1
        if dfs:
            df = pd.concat(dfs)
        else:
            df = pd.DataFrame([],columns= colunas_padrao)


        df.replace("NAO ENCONTRADO", "", inplace=True)
        return df
    
    ini = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        dfs = list(executor.map(processa_arquivo, arquivos_para_ler))
    if dfs:
        df = pd.concat(dfs)
    else:
        df = pd.DataFrame([],columns= colunas_padrao)
    
    ini = time.time()
    df["cnpj"] = df["cnpj"].apply(lambda x: re.sub(r'[^0-9]', '', x))

    ini = time.time()
    df = padronizacao_mailing_final(df).reset_index(drop=True)

    if filtro_telefone_blacklist == "apenas_filtrados":
        df = filtra_mailing(df)
    
    colunas_telefone = ["TEL1", "TEL2", "TEL3"] + [f"Telefone_{i}" for i in range(1,21)]
    cols = df.columns.tolist()

    ini = time.time()
    for col_tel in colunas_telefone:
        if col_tel not in cols:
            df[col_tel] = ""

    apenas_celular = tipos_telefone == "apenas_movel"
    df[colunas_telefone] = df[colunas_telefone].applymap(
        lambda x: clean_phone_number(x, apenas_celular=apenas_celular)
    )
    apenas_fixos = tipos_telefone == "apenas_fixos"
    df[colunas_telefone] = df[colunas_telefone].applymap(
        lambda x: clean_phone_number(x, apenas_fixos=apenas_fixos)
    )
    
    ini = time.time()
    #garantindo que telefones sempre fiquem à esquerda
    df = compacta_colunas(df, ["TEL1", "TEL2", "TEL3"] + [f"Telefone_{i}" for i in range(1,21)])
    
    ini = time.time()
    if formato_saida == "IPBOX":
        colunas_telefone = ["TEL1", "TEL2", "TEL3"] + [f"Telefone_{i}" for i in range(1,6)]

        i = 1
        for ct in colunas_telefone:
            df[f'DDD{i}'] = df[ct].str[:2]
            df[f"TEL{i}"] = df[ct].str[2:]
            i+=1

        df = df[colunas_ip_box]

        
    if formato_saida == "VONIX":
        colunas_telefone = ["TEL1", "TEL2", "TEL3"] + [f"Telefone_{i}" for i in range(1,6)]
        i = 1
        for ct in colunas_telefone:
            df[f"TELEFONE{i}"] = df[ct].apply(lambda x: x if clean_phone_number(x) else "")
            i+=1

        df.rename(columns={
            "cnpj": "CNPJ",
            "razao_social": "NOME",
            "decisor": "DECISOR",
            "correio_eletronico": "EMAIL",
            "logradouro": "LOGRADOURO",
            "num_fachada": "NUMERO",
            "complemento1": "COMPLEMENTO",
            "cep": "CEP",
            "bairro": "BAIRRO ALTO",
            "municipio": "CIDADE",
            "uf": "UF",
        }, inplace=True)

        df["ORIGEM"] = df["MEINAOMEI"].apply(lambda x: "MEI" if str(x).strip().lower()=="s" else "NMEI" )  


        df = df[colunas_vonix]

    
    return df


def get_infos_cnpj(cnpj, estado) -> dict:
    def mascara_cnpj(cnpj: str) -> str:
        cnpj = ''.join(filter(str.isdigit, cnpj))  # mantém só números
        return f"{cnpj[0:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

    cnpj_numeros = re.sub(r'\D', '', str(cnpj))
    cnpj_numeros = cnpj_numeros.zfill(14)
    cnpj_mascara = mascara_cnpj(cnpj_numeros)
    dados_base = {}
    
    #procurando pelos dados base

    df_base = pd.read_csv(os.path.join(os.getcwd(), f'media/arquivos_receita_federal/{estado}.csv'), sep=";")

    df_base = df_base[df_base["cnpj"] == cnpj_mascara]
    if len(df_base.index) <=0:
        return {}
    for _,row in df_base.iterrows():
        dados_base = row.to_dict()
    
    dados_base["credito"] = "SEM INFOS"
    dados_base["tipo_de_viabilidade"] = ""

    filepaths = [
        os.path.join(os.getcwd(), 'media/viabilidades_credito_enriquecido'),
        os.path.join(os.getcwd(), 'media/viabilidades_credito_nao_informado_enriquecido'),
        os.path.join(os.getcwd(), 'media/viabilidades_credito_pre_negado_enriquecido'),
    ]
    arquivos_para_ler = []

    for fp in filepaths:
        for f in os.listdir(fp):
            df_path = os.path.join(fp, f)
            if estado not in df_path:
                continue
            if f.endswith(".csv"):
                arquivos_para_ler.append(df_path)

    for f in arquivos_para_ler:

        df = pd.read_csv(f, sep=";", dtype={"data_inicio_atividades":"string", "natureza_juridica": "category", "descricaonj":"category", "cnae_fiscal":"string","cnae_fiscal_secundaria":"string", "descricaocf":"category", "cnpj":"string", "razao_social":"string", "nome_fantasia": "string", "matriz_filial":"category", "decisor":"string", "situacao_cadastral":"category", "correio_eletronico":"string", "logradouro":"string", "num_fachada":"string", "complemento1":"string", "bairro":"string", "cep":"string", "municipio":"category","uf":"category", "CPF":"string", "MEINAOMEI": "category", "TEL1":"string", "TEL2":"string", "TEL3":"string"} , on_bad_lines="skip", skip_blank_lines=True,)
        df = df[df["cnpj"] == cnpj_numeros]
        if len(df.index) <=0:
            continue

        print(f'ACHEI O CNPJ EM {f}')
        for _,row in df.iterrows():
            print(f)
            if "viabilidades_credito_enriquecido" in f:
                dados_base["credito"] = "APROVADO"
            elif "viabilidades_credito_nao_informado_enriquecido" in f:
                dados_base["credito"] = "SEM INFOS"
            elif "viabilidades_credito_pre_negado_enriquecido" in f:
                dados_base["credito"] = "NEGADO"

            

            if "Secundaria" in f:
                dados_base["tipo_de_viabilidade"] = "SECUNDÁRIA"
            else:
                dados_base["tipo_de_viabilidade"] = "PRIMÁRIA"

            return dados_base
    
    return dados_base


def get_dados_csv(colunas_filtro:dict, campos_retorno:list=[]) -> pd.DataFrame:
    filepath = os.path.join(os.getcwd(), 'media/arquivos_receita_federal')
    arquivos_para_ler = []

    for f in os.listdir(filepath):
        df_path = os.path.join(filepath, f)
        if "uf" in colunas_filtro:
            if f.split(".")[0] not in colunas_filtro["uf"]:
                continue
        if f.endswith(".csv"):
            arquivos_para_ler.append(df_path)
    
    def processa_arquivo(df_path:str, colunas_filtro=colunas_filtro, campos_retorno=campos_retorno) -> pd.DataFrame:
        colunas_padrao = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3"]
        dtypes = {
            'uf': "category",
            'municipio': "category",
            'cnae_fiscal': 'string',
            'bairro': 'category',
            'natureza_juridica': 'string',
            'situacao_cadastral': "category",
            'MEINAOMEI': "string",
            "TEL1": 'string',
            "TEL2": 'string',
            "TEL3": 'string'
        }
        
        campos_retorno = campos_retorno if campos_retorno!= [] else colunas_padrao
        dfs = []
        
        for chunk in pd.read_csv(df_path, sep=";", names=colunas_padrao, chunksize=1_000_000, usecols=colunas_padrao, dtype=dtypes):
            mask = pd.Series(True, index=chunk.index)

            for coluna, valores in colunas_filtro.items():
                if coluna == "cnae_fiscal":
                    valores = [str(v).split(" - ")[0] for v in valores]
                
                if coluna == "termos_chave":
                    # Adicionando filtro por termo em nome_fantasia ou razao_social
                    if valores:  # Garante que não está vazio
                        if isinstance(valores, str):
                            termos = [valores]
                        else:
                            termos = valores

                        termos_mask = pd.Series(False, index=chunk.index)
                        for termo in termos:
                            termo = str(termo).strip()
                            if termo:
                                termos_mask |= (
                                    chunk["nome_fantasia"].astype(str).str.contains(termo, case=False, na=False) |
                                    chunk["razao_social"].astype(str).str.contains(termo, case=False, na=False)
                                )
                        mask &= termos_mask
                    continue  # pula para o próximo item do for
                
                if coluna == "MEINAOMEI":
                    chunk[coluna].fillna("", inplace=True)
                    if valores == "N":
                        valores = ["", "N"]
                    else:
                        valores = ["S", ]

                mask &= chunk[coluna].astype(str).isin(valores)

            chunk = chunk[mask]
                        
            if not chunk.empty:
                dfs.append(chunk)

        if dfs:
            df = pd.concat(dfs)
        else:
            df = pd.DataFrame([],columns= colunas_padrao)
        df = df[campos_retorno]

        
        return df

    with ThreadPoolExecutor(max_workers=2) as executor:
        dfs = list(executor.map(processa_arquivo, arquivos_para_ler))

    if dfs:
        df = pd.concat(dfs)
    else:
        df = pd.DataFrame([],columns= colunas_padrao)
        
    return df

def get_municipios_estado(uf, ):
    df_municipio = get_dados_csv({"uf": [uf, ],}, ["municipio",]).dropna(subset=["municipio"])
    municipios = df_municipio["municipio"].unique().tolist()
    return municipios

def get_cnaes() -> list:
    cnaes_path = os.path.join(os.getcwd(), 'media/dados_cnaes.csv')
    cnaes = list(sorted(pd.read_csv(cnaes_path, sep=";",  )["cnae_desc"].unique().tolist()))
    return cnaes

# get_dados_csv({"uf": ["AC"], "cnae_fiscal": ["4744099"]})


def verifica_ultima_att_receita():

    try:
        hoje = datetime.today()
        mes_passado = hoje - relativedelta(months=0)
        data_formatada = mes_passado.strftime("%Y-%m")

        url_base = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{data_formatada}/"
        resposta = requests.get(url_base, timeout=8)
        if str(resposta.status_code) == "404":
            hoje = datetime.today()
            mes_passado = hoje - relativedelta(months=1)
            data_formatada = mes_passado.strftime("%Y-%m")
            return data_formatada
    except:
        data_formatada = "00-00"

    return data_formatada

def limpa_pasta(pasta):
    for file in os.listdir(pasta):
        filename = os.path.join(pasta, file)
        if os.path.exists(filename):
            os.remove(filename)



