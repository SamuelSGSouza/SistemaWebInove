import pandas as pd
from data.models import *
import os, traceback, re
from functions.contantes import *
from pathlib import Path
from datetime import datetime, timedelta
from functions.utils import _detectar_encoding_csv, _detectar_sep_csv
from corpdata.models import Empresa
import unicodedata
import hashlib
from django.db import transaction


# ----------------------------------------------------------------------------
# 1. NORMALIZAÇÃO
# ----------------------------------------------------------------------------
_UNIDADES = {
    "LJ": "LOJA", "LOJA": "LOJA", "SL": "SALA", "SALA": "SALA",
    "AP": "APTO", "APT": "APTO", "APTO": "APTO", "APARTAMENTO": "APTO",
    "CS": "CASA", "CASA": "CASA", "BL": "BLOCO", "BLOCO": "BLOCO",
    "AND": "ANDAR", "ANDAR": "ANDAR", "GAL": "GALPAO", "GALPAO": "GALPAO",
    "QD": "QUADRA", "LT": "LOTE", "LOTE": "LOTE", "SOBRELOJA": "SOBRELOJA",
}
_TIPOS_LOGRADOURO = {
    "R": "RUA", "RUA": "RUA",
    "AV": "AVENIDA", "AVE": "AVENIDA", "AVENIDA": "AVENIDA",
    "TV": "TRAVESSA", "TRAV": "TRAVESSA", "TRAVESSA": "TRAVESSA",
    "ROD": "RODOVIA", "RODOVIA": "RODOVIA",
    "EST": "ESTRADA", "ESTR": "ESTRADA", "ESTRADA": "ESTRADA",
    "PC": "PRACA", "PCA": "PRACA", "PRACA": "PRACA",
    "AL": "ALAMEDA", "ALAMEDA": "ALAMEDA",
    "LG": "LARGO", "LARGO": "LARGO",
    "VL": "VILA", "VILA": "VILA",
    "CJ": "CONJUNTO", "CONJ": "CONJUNTO", "CONJUNTO": "CONJUNTO",
    "JD": "JARDIM", "JDIM": "JARDIM", "JARDIM": "JARDIM",
    "QD": "QUADRA", "QUADRA": "QUADRA",
    "LT": "LOTE", "LOTE": "LOTE",
}
 
_ABREVIACOES = {
    "PRES": "PRESIDENTE", "PRESID": "PRESIDENTE",
    "DR": "DOUTOR", "DRA": "DOUTORA",
    "PROF": "PROFESSOR", "PROFA": "PROFESSORA",
    "CEL": "CORONEL", "CAP": "CAPITAO", "GEN": "GENERAL", "MAJ": "MAJOR",
    "GOV": "GOVERNADOR", "SEN": "SENADOR", "DEP": "DEPUTADO",
    "MIN": "MINISTRO", "MAL": "MARECHAL", "ENG": "ENGENHEIRO",
    "S": "SAO", "STA": "SANTA", "STO": "SANTO", "SA": "SAO",
    "N": "NOSSA", "NSA": "NOSSA", "SRA": "SENHORA",
    "PE": "PADRE", "MONS": "MONSENHOR", "IRM": "IRMAO",
    "VER": "VEREADOR", "DES": "DESEMBARGADOR", "COM": "COMENDADOR",
}
 
# datas viram por extenso na Receita e em algarismo no DFV (e vice-versa)
_NUMERAIS = {
    "1": "PRIMEIRO", "2": "DOIS", "3": "TRES", "4": "QUATRO", "5": "CINCO",
    "6": "SEIS", "7": "SETE", "8": "OITO", "9": "NOVE", "10": "DEZ",
    "11": "ONZE", "12": "DOZE", "13": "TREZE", "14": "QUATORZE",
    "15": "QUINZE", "16": "DEZESSEIS", "17": "DEZESSETE", "18": "DEZOITO",
    "19": "DEZENOVE", "20": "VINTE", "21": "VINTEEUM", "25": "VINTEECINCO",
    "28": "VINTEEOITO", "29": "VINTEENOVE", "30": "TRINTA", "31": "TRINTAEUM",
    "I": "PRIMEIRO", "II": "DOIS", "III": "TRES", "IV": "QUATRO", "V": "CINCO",
    "1O": "PRIMEIRO", "1º": "PRIMEIRO", "7O": "SETE", "7º": "SETE",
}
 
_STOPWORDS = {"DE", "DA", "DO", "DAS", "DOS", "E", "EM", "A", "O"}

#################################################################
########################### HELPERS #############################
#################################################################
def _sem_acento(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s)
                   if not unicodedata.combining(c))

def normaliza_numero(serie: pd.Series) -> pd.Series:
    """Primeiro grupo numérico, sem zero à esquerda. '84 A'->'84', '0084'->'84',
    '123/125'->'123', 'S/N'->''. (o \\D atual transformaria '123/125' em '123125')"""
    s = serie.astype("string").fillna("")
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.extract(r"(\d+)", expand=False).fillna("")
    return s.str.replace(r"^0+(?=\d)", "", regex=True)

def normaliza_logradouro(serie: pd.Series) -> pd.Series:
    """'AVENIDA PRES MEDICE CONJ CANAA' -> 'AVENIDA CANAA CONJUNTO MEDICE PRESIDENTE'
    (tokens ordenados: imune a ordem, abreviação, acento, pontuação e stopword)"""
    def _norm(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        t = _sem_acento(str(v)).upper()
        t = re.sub(r"[^A-Z0-9 ]", " ", t)
        toks = []
        for i, tok in enumerate(t.split()):
            if i == 0 and tok in _TIPOS_LOGRADOURO:
                toks.append(_TIPOS_LOGRADOURO[tok])
                continue
            tok = _ABREVIACOES.get(tok, tok)
            tok = _TIPOS_LOGRADOURO.get(tok, tok)
            tok = _NUMERAIS.get(tok, tok)
            if tok and tok not in _STOPWORDS:
                toks.append(tok)
        return " ".join(sorted(set(toks)))
    return serie.map(_norm).astype("string")

def normaliza_cep(serie: pd.Series) -> pd.Series:
    """Só dígitos, 8 posições, zero à esquerda. '69906-107'/'69906107.0' -> '69906107'."""
    s = serie.astype("string").fillna("")
    s = s.str.replace(r"\.0$", "", regex=True)          # float virado string
    s = s.str.replace(r"\D", "", regex=True)
    s = s.where(s.str.len().between(7, 8), "")          # descarta lixo
    return s.str.zfill(8).replace("00000000", "")

def normaliza_complemento(serie: pd.Series) -> pd.Series:
    """'LJ 2' -> 'LOJA2' ; 'SALA 301' -> 'SALA301' ; vazio -> ''."""
    def _norm(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        t = _sem_acento(str(v)).upper()
        if t in ("NAN", "NONE"):
            return ""
        t = re.sub(r"[^A-Z0-9 ]", " ", t)
        partes = []
        for m in re.finditer(r"\b([A-Z]+)\.?\s*0*(\d+)", t):
            tipo = _UNIDADES.get(m.group(1))
            if tipo:
                partes.append(f"{tipo}{m.group(2)}")
        return "".join(sorted(set(partes)))
    return serie.map(_norm).astype("string")

def padroniza(df:pd.DataFrame, cep:str, numero:str, logradouro:str, complemento=None, prefixo="") -> pd.DataFrame:
    df[cep] = normaliza_cep(df[cep])
    df[numero] = normaliza_numero(df[numero])
    df[logradouro] = normaliza_logradouro(df[logradouro])
    df[complemento] = normaliza_complemento(df[complemento])

    return df

def gera_campos_cep(df: pd.DataFrame,
                    campo_cep: str,
                    campo_numero: str,
                    campo_logradouro: str,
                    campo_especifico = None,
                    usar_chave_geral=True) -> pd.DataFrame:
    """
    CHAVE_ESPECIFICA = cep + <campo_especifico>          (só quando o CEP é específico)
    CHAVE_GERAL      = cep + hash(logradouro) + numero   (só quando o CEP é genérico)

    campo_especifico troca o discriminador do endereço: número da fachada no
    padrão nacional, complemento/lote em DF e GO. Default = campo_numero.

    Garantia: chave vazia == chave inválida. Quem consome não precisa validar
    comprimento nem refiltrar por tipo de CEP.
    """
    campo_especifico = campo_especifico or campo_numero
    def _txt(serie: pd.Series) -> pd.Series:
        """Série -> str puro, com NA/NaN virando '' (evita a string 'nan' na chave)."""
        return serie.astype("string").fillna("").astype(str)
    
    cep   = _txt(df[campo_cep])
    num   = _txt(df[campo_numero])
    logr  = _txt(df[campo_logradouro])
    espec = _txt(df[campo_especifico])

    # hash só dos logradouros distintos — são poucos milhares, contra milhões de linhas
    unicos = logr.unique()
    mapa = {s: (hashlib.blake2s(s.encode(), digest_size=3).hexdigest() if s else "")
            for s in unicos}
    logr_hash = logr.map(mapa)

    df["CHAVE_ESPECIFICA"] = cep + "|" + espec
    

    sem_cep      = cep.eq("")
    cep_generico = cep.str.endswith("000")
    sem_espec    = espec.eq("")
    sem_num      = num.eq("")
    sem_logr     = logr.eq("")

    if usar_chave_geral:
        df["CHAVE_GERAL"] = cep + "|" + logr_hash + "|" + num
        df.loc[sem_cep | ~cep_generico | sem_num | sem_logr, "CHAVE_GERAL"] = ""
    else:
        df["CHAVE_GERAL"] = ""

    df.loc[sem_cep | cep_generico | sem_espec, "CHAVE_ESPECIFICA"] = ""
    df.loc[sem_cep | ~cep_generico | sem_num | sem_logr, "CHAVE_GERAL"] = ""

    return df


def pega_lote(string) ->str:
    padrao = r'\bLOTE\s*\w+'
    match = re.search(padrao, string)
    if match:
        lote = match.group()
        lote = re.sub(r"\s*", "", lote).strip().replace("LOTE", "LT").replace("nan", "")
        return lote
    return ""

def _update_em_lotes(qs_base, ids, valor, tamanho=5_000) -> int:
    total = 0
    for i in range(0, len(ids), tamanho):
        total += qs_base.filter(id__in=ids[i:i + tamanho]).update(viabilidade=valor)
    return total


def fase_2_concatenador_DB(sistema, nova_execucao:Status_Execucoe_DB):
    salva_status(nova_execucao, f"Iniciando processo para salvar viabilidades no banco", status="Em Andamento")

    dtype={"HP_LIVRE": int, "CEP": "string"}
    path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")

    for estado in ESTADOS_BR:
        estado_por_lote = estado in ("DF", "GO")     
        salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
        qs = Empresa.objects.filter(municipio__uf=estado).values(
            "id", "cnpj", "cep", "numero", "logradouro", "complemento"
        )
        df_receita = pd.DataFrame(qs)
        if df_receita.empty:
            salva_status(nova_execucao,f"Não foram encontradas empresas na receita federal para o estado {estado}", status="Em Andamento")
            continue

        for c in ("cep", "numero", "logradouro", "complemento"):
            df_receita[f"raw_{c}"] = df_receita[c]
        
        df_receita = padroniza(df_receita, "cep", "numero", "logradouro", "complemento")
        df_receita = gera_campos_cep(
            df_receita, "cep", "numero", "logradouro",
            campo_especifico="complemento" if estado_por_lote else "numero",
            usar_chave_geral=not estado_por_lote
        )     


        dfs_dfv = []
        for file in os.listdir(path_arquivos_dfv):
            if file.startswith(estado) and file.lower().endswith((".xlsb", ".xlsx")): # os nomes dos arquivos já são padronizados na fase anterior, ficando como AC., BA. ...
                df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
                dfs_dfv.append(df_dfv_estado)
        if not dfs_dfv:
            salva_status(nova_execucao,f"Não foram encontrados dados de viabilidade para o estado {estado}", status="Em Andamento")
            continue
        
        campo_complemento_dfv = "COMPLEMENTO1" if estado != "GO" else "COMPLEMENTO2"

        
        df_dfv = pd.concat(dfs_dfv, ignore_index=True)
        for c in ("CEP", "NO_FACHADA", "LOGRADOURO", campo_complemento_dfv):
            df_dfv[f"raw_{c}"] = df_dfv[c]
        df_dfv = padroniza(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO", campo_complemento_dfv)
        df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1].copy()
        df_dfv = gera_campos_cep(
            df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO",
            campo_especifico=campo_complemento_dfv if estado_por_lote else "NO_FACHADA",
            usar_chave_geral=not estado_por_lote,
        )

        chaves_especificas_dfv = {c for c in df_dfv["CHAVE_ESPECIFICA"].unique() if c}
        chaves_geral_dfv       = {c for c in df_dfv["CHAVE_GERAL"].unique() if c}

        df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
        df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]

        df_receita_viaveis:pd.DataFrame = pd.concat([df_receita_cep_especifico, df_receita_cep_geral])
        cnpjs_viabilidade_primaria = df_receita_viaveis["cnpj"].unique().tolist()
        ids_primaria = df_receita_viaveis["id"].unique().tolist()
        

        cep_dfv = df_dfv["CEP"]
        ceps_especificos_dfv = set(cep_dfv[cep_dfv.ne("") & ~cep_dfv.str.endswith("000")].unique())

        df_receita_nao_coletados = df_receita[~df_receita["id"].isin(df_receita_viaveis["id"])]

        df_receita_mailing_secundario = df_receita_nao_coletados[
            df_receita_nao_coletados["cep"].ne("")
            & df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)
        ]
        padrao = r'^(APTO|APARTAMENTO|SALA|BLOCO|CONJUNTO|ANDAR)\d'
        df_receita_mailing_secundario = df_receita_mailing_secundario[
            ~df_receita_mailing_secundario['complemento']
            .fillna('')
            .str.contains(padrao, case=False, regex=True)
        ]
        ids_secundaria = df_receita_mailing_secundario["id"].unique().tolist()


        qs_estado = Empresa.objects.filter(municipio__uf=estado)
        
        with transaction.atomic():
            qs_estado.update(viabilidade=None)   # 1 query, zero parâmetros
            n1 = _update_em_lotes(qs_estado, ids_primaria,   Empresa.VIABILIDADE_PRIMARIA)
            n2 = _update_em_lotes(qs_estado, ids_secundaria, Empresa.VIABILIDADE_SECUNDARIA)

                
        salva_status(nova_execucao,
            f"{estado}: DFV {df_dfv['CHAVE_ESPECIFICA'].ne('').sum()}/{len(df_dfv)} esp, "
            f"{df_dfv['CHAVE_GERAL'].ne('').sum()} geral | "
            f"Receita {df_receita['CHAVE_ESPECIFICA'].ne('').sum()}/{len(df_receita)} esp | "
            f"match {len(cnpjs_viabilidade_primaria)}",
            status="Em Andamento")
    
                

def fase_2_concatenador(sistema, nova_execucao:Status_Execucoe_DB):
    pasta_receita_federal = os.path.join(os.getcwd(), "media", "arquivos_receita_federal")
    salva_status(nova_execucao, f"Iniciando análise de viabilidades para o sistema {sistema}", status="Em Andamento")
    fase_2_concatenador_DB(sistema, nova_execucao)

    # if sistema == "oi":
    #     try:
    #         # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
    #         dtype={"HP_LIVRE": int, "CEP": "string"}
    #         path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
    #         path_viabilidades = os.path.join(os.getcwd(), "media", "viabilidades")

    #         for file in os.listdir(path_viabilidades):
    #             os.remove(os.path.join(path_viabilidades, file))

    #         for estado in ESTADOS_BR:        
    #             salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
    
    #             df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
    #             df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")
    #             df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))

    #             df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

    #             dfs_dfv = []
    #             for file in os.listdir(path_arquivos_dfv):
    #                 if estado in file:
    #                     print(f"ARQUIVO de DFV: {file}")
    #                     df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
    #                     dfs_dfv.append(df_dfv_estado)

    #             df_dfv = pd.concat(dfs_dfv)

    #             df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1]



    #             df_dfv = gera_campos_cep(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO")

    #             dfv_mailings_viaveis = []
    #             if estado in ["DF", "GO"]:
    #                 campo_complemento_dfv = "COMPLEMENTO1" if estado == "DF" else "COMPLEMENTO2"

    #                 df_receita["lote"] = df_receita["complemento1"].apply(lambda x: pega_lote(str(x)))
    #                 df_receita["CHAVE_ESPECIFICA"] = df_receita["cep"] + df_receita["lote"]

                    
    #                 df_dfv["CHAVE_ESPECIFICA"] = df_dfv["CEP"] + df_dfv[campo_complemento_dfv].astype(str).str.replace(" ", "").replace("nan", "")
    #                 chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
    #                 chaves_especificas_dfv = [str(c) for c in chaves_especificas_dfv if len(str(c))>9]

    #                 df_chaves_lote_df = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]

                    
                    
    #                 df_receita["numero_tratado"] = df_receita["num_fachada"].apply(lambda x: re.sub(r'\D', '', str(x)))
    #                 df_receita["CHAVE_GERAL"] = df_receita["cep"] + df_receita["logradouro"].astype(str).str.replace(" ", "").str[-3:] + df_receita["numero_tratado"]
                    
    #                 df_dfv["numero_tratado"] = df_dfv[campo_complemento_dfv].apply(lambda x: re.sub(r'\D', '', str(x)))
    #                 df_dfv["CHAVE_GERAL"] = df_dfv["CEP"] + df_dfv["LOGRADOURO"].astype(str).str.replace(" ", "").str[-3:] + df_dfv["numero_tratado"]
                    
    #                 chaves_gerais_dfv = df_dfv["CHAVE_GERAL"].unique().tolist()
    #                 chaves_gerais_dfv = [str(c) for c in chaves_gerais_dfv if len(str(c))>9]

    #                 df_chaves_gerais = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_gerais_dfv)]


                    

    #                 dfv_mailings_viaveis.append(df_chaves_lote_df)
    #                 dfv_mailings_viaveis.append(df_chaves_gerais)

                    


    #             else:

    #                 chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
    #                 chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

    #                 chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
    #                 chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

    #                 df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
    #                 df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]

    #                 dfv_mailings_viaveis.append(df_receita_cep_especifico)
    #                 dfv_mailings_viaveis.append(df_receita_cep_geral)


    #             df_receita_viaveis:pd.DataFrame = pd.concat(dfv_mailings_viaveis)

    #             df_receita_viaveis.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)
    #             df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)

    #             ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

    #             df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]

    #             df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
    #             padrao = r'\b(apto|apartamento|sala|bloco)\b'
    #             df_receita_mailing_secundario = df_receita_mailing_secundario[
    #                 ~df_receita_mailing_secundario['complemento1']
    #                 .fillna('')
    #                 .str.contains(padrao, case=False, regex=True)
    #             ]
                
    #             df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)


            
                    

    #     except Exception as e:
    #         salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} não possui as colunas esperadas",status="Erro")            
    #         return False

    # elif sistema == "giga_mais":
    #     dtype={"CEP": "string"}
    #     path_arquivos_dfv = os.path.join(os.getcwd(), "media_giga_mais", "arquivos_dfv")
    #     path_viabilidades = os.path.join(os.getcwd(), "media_giga_mais", "viabilidades")

    #     for file in os.listdir(path_viabilidades):
    #         os.remove(os.path.join(path_viabilidades, file))

    #     dfs_dfv = []
    #     for file in os.listdir(path_arquivos_dfv):
    #         df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
    #         dfs_dfv.append(df_dfv_estado)
    #     df_dfv = pd.concat(dfs_dfv)

    #     df_dfv["cep_geral"] = df_dfv["CEP"].apply(lambda x: str(x).endswith("000"))
    #     df_dfv = df_dfv[df_dfv["cep_geral"] != True]

    #     ceps_permitidos = df_dfv["CEP"].unique().tolist()
    #     for estado in ESTADOS_BR:        
    #         salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")

            

    #         df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
    #         df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")
    #         df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))

    #         df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

            

            

            


    #         df_receita_viaveis = df_receita[df_receita["cep"].isin(ceps_permitidos)]
    #         padrao = r'\b(apto|apartamento|sala|bloco)\b'
    #         df_receita_viaveis = df_receita_viaveis[
    #                 ~df_receita_viaveis['complemento1']
    #                 .fillna('')
    #                 .str.contains(padrao, case=False, regex=True)
    #             ]
    #         df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)

    # elif sistema == "janeiro_2026":
    #     try:
            
    #         cnpjs_ja_coletados = []
    #         pasta_cnpjs_coletados = os.path.join(os.getcwd(), "media", "viabilidades")
    #         for file in os.listdir(pasta_cnpjs_coletados):
    #             filepath = os.path.join(pasta_cnpjs_coletados, file)

    #             df_coletado = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)
    #             df_coletado["cnpj"] = df_coletado["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
    #             cnpjs = df_coletado["cnpj"].unique().tolist()
    #             cnpjs_ja_coletados+= cnpjs

    #         # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
    #         dtype={"CEP": "string", "FACHADA": "string", "ENDERECO":"string"}
    #         path_arquivos_dfv = os.path.join(os.getcwd(), "media_janeiro_2026", "arquivos_dfv")
    #         path_viabilidades = os.path.join(os.getcwd(), "media_janeiro_2026", "viabilidades")

    #         for file in os.listdir(path_viabilidades):
    #             os.remove(os.path.join(path_viabilidades, file))

    #         for estado in ESTADOS_BR:        
    #             salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
    
    #             df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
    #             df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
    #             df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")

    #             df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

    #             dfs_dfv = []
    #             for file in os.listdir(path_arquivos_dfv):
    #                 if estado in file:

    #                     df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
    #                     dfs_dfv.append(df_dfv_estado)
                
    #             if len(dfs_dfv) < 1:
    #                 salva_status(nova_execucao, f"Nenhum dfv encontrado para o estado {estado}", status="Erro")
    #                 return False

    #             df_dfv = pd.concat(dfs_dfv)

    #             df_dfv = gera_campos_cep(df_dfv, "CEP", "FACHADA", "ENDERECO")

    #             chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
    #             chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

    #             chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
    #             chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

    #             df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
    #             df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]


    #             df_receita_viaveis:pd.DataFrame = pd.concat([df_receita_cep_especifico, df_receita_cep_geral])

    #             df_receita_viaveis.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)
    #             df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)

    #             ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

    #             df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]

    #             df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
    #             padrao = r'\b(apto|apartamento|sala|bloco)\b'

    #             df_receita_mailing_secundario = df_receita_mailing_secundario[
    #                 ~df_receita_mailing_secundario['complemento1']
    #                 .fillna('')
    #                 .str.contains(padrao, case=False, regex=True)
    #             ]
                
    #             df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)




            
                    

    #     except Exception as e:
    #         print(traceback.format_exc())
    #         salva_status(nova_execucao, titulo=f"{e}",status="Erro")            
    #         return False

    # elif sistema == "mailing_cpfs":
    #     COLUNAS_CPF=["cpf", "nome", "endereco", "numero", "complemento","cep", "bairro","cidade", "uf", "celular_1", "celular_2", "celular_3", "renda_presumida"]
    #     paths_arquivos_cpf = [
    #         os.path.join(os.getcwd(), "media_mailing_cpf", "arquivos_cpf_externo"),
    #         os.path.join(os.getcwd(), "media_mailing_cpf", "arquivos_cpf_credlink"),
    #     ]
    #     path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
    #     path_viabilidades = os.path.join(os.getcwd(), "media_mailing_cpf", "viabilidades")
    #     os.makedirs(path_viabilidades, exist_ok=True)
    #     dtype={"HP_LIVRE": int, "CEP": "string"}
    #     for file in os.listdir(path_viabilidades):
    #         os.remove(os.path.join(path_viabilidades, file))
    
    #     for estado in ESTADOS_BR:        
    #         salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
            
    #         dfs_dfv = []
    #         for file in os.listdir(path_arquivos_dfv):
    #             if str(estado).lower() in str(file).lower():

    #                 df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
    #                 dfs_dfv.append(df_dfv_estado)

    #         df_dfv = pd.concat(dfs_dfv)
    #         df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1]
    #         df_dfv = gera_campos_cep(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO")

    #         chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
    #         chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

    #         chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
    #         chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

    #         dfs_receita = []
    #         for pasta in paths_arquivos_cpf:
    #             os.makedirs(pasta, exist_ok=True)
    #             for file in os.listdir(pasta):
    #                 if estado in file:
    #                     filename = os.path.join(pasta, file)
    #                     print(f"USANDO ARQUIVO: {filename}")
    #                     ext = os.path.splitext(filename)[1].lower()

    #                     if ext in (".csv", ".txt"):
    #                         chunks = pd.read_csv(filename, sep=_detectar_sep_csv(filename), dtype=str, encoding=_detectar_encoding_csv(filename),on_bad_lines="skip", chunksize=1_000_000)
    #                     elif ext in (".xls", ".xlsx", ".xlsb"):
    #                         chunks = pd.read_excel(filename, dtype=str, chunksize=1_000_000)
    #                     else:
    #                         salva_status(nova_execucao, titulo=f"Erro analisar cnpjs com viabilidade. Arquivo {file} está num formato desconhecido",status="Erro")   
    #                         return False    
    #                     for df_cpf in chunks:
    #                         df_cpf.columns = df_cpf.columns.str.lower()
    #                         df_cpf.rename(columns={
    #                                 "logradouro": "endereco",
    #                                 "ENDERECO": "endereco",
    #                                 "celular1": "celular_1",
    #                                 "CEL_1": "celular_1",
    #                                 "celular2": "celular_2",
    #                                 "CEL_2": "celular_2",
    #                                 "celular3": "celular_3",
    #                                 "CEL_3": "celular_3",
    #                                 "renda pressumida": "renda_pressumida",
    #                                 "RENDA": "renda_pressumida",

    #                             }, inplace=True)

    #                         if "ddd1" in df_cpf.columns.tolist():
    #                             df_cpf["celular_1"] = df_cpf["ddd1"] + df_cpf["tel1"]
    #                             df_cpf["celular_2"] = df_cpf["ddd2"] + df_cpf["tel2"]
    #                             df_cpf["celular_3"] = df_cpf["ddd3"] + df_cpf["tel3"]
    #                             df_cpf["renda_presumida"] = ""

    #                         if "complemento" not in df_cpf.columns.to_list():
    #                             df_cpf["complemento"] = ""
    #                         df_cpf = df_cpf[COLUNAS_CPF]
    #                         df_cpf = gera_campos_cep(df_cpf, "cep", "numero", "endereco")
    #                         df_cpf["cpf"] = df_cpf["cpf"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(11))
    #                         df_cpf.drop_duplicates(subset=["cpf"], keep="first", inplace=True)
                            
    #                         df_cpf["pasta"] = str(pasta).split("/")[-1]


    #                         df_cpf_estado_cep_especifico = df_cpf[df_cpf["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
    #                         df_cpf_estado_cep_geral = df_cpf[df_cpf["CHAVE_GERAL"].isin(chaves_geral_dfv)]


    #                         df_cpf_estado_viaveis:pd.DataFrame = pd.concat([df_cpf_estado_cep_especifico, df_cpf_estado_cep_geral])

                            
    #                         nome_arquivo = os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv")
    #                         write_header = not os.path.exists(nome_arquivo)
    #                         df_cpf_estado_viaveis.to_csv(nome_arquivo, mode="a", header=write_header, index=False, sep=";", encoding="utf-8")



    #                         ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

    #                         df_cpf_estado_nao_coletados = df_cpf[~df_cpf["CHAVE_ESPECIFICA"].isin(df_cpf_estado_viaveis["CHAVE_ESPECIFICA"].unique().tolist())]

    #                         df_cpf_estado_mailing_secundario = df_cpf_estado_nao_coletados[df_cpf_estado_nao_coletados["cep"].isin(ceps_especificos_dfv)]
    #                         padrao = r'\b(apto|apartamento|sala|bloco)\b'
    #                         df_cpf_estado_mailing_secundario = df_cpf_estado_mailing_secundario[
    #                             ~df_cpf_estado_mailing_secundario['complemento']
    #                             .fillna('')
    #                             .str.contains(padrao, case=False, regex=True)
    #                         ]

    #                         nome_arquivo = os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv")
    #                         write_header = not os.path.exists(nome_arquivo)
    #                         df_cpf_estado_mailing_secundario.to_csv(nome_arquivo, mode="a", header=write_header, index=False, sep=";", encoding="utf-8")


    #         nome_arquivo_primario = os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv")
    #         colunas_esperadas = COLUNAS_CPF + ["CHAVE_ESPECIFICA", "CHAVE_GERAL", "pasta"]
    #         texto_colunas_esperadas = ";".join(colunas_esperadas)
    #         if not os.path.exists(nome_arquivo_primario):
    #             with open(nome_arquivo_primario, "w", encoding="utf-8") as arq:
    #                 arq.write(texto_colunas_esperadas)

    #         nome_arquivo_secundario = os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv")
    #         if not os.path.exists(nome_arquivo_secundario):
    #             with open(nome_arquivo_secundario, "w", encoding="utf-8") as arq:
    #                 arq.write(texto_colunas_esperadas)

            
                        
    #     return verificador_fase_2_cpf(sistema, nova_execucao)
         

            

    # return verificador_fase_2(sistema, nova_execucao)

def verificador_fase_2_cpf(sistema, nova_execucao):
    estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    COLUNAS_CPF=["cpf", "nome", "endereco", "numero", "complemento","cep", "bairro","cidade", "uf", "celular_1", "celular_2", "celular_3", "renda_presumida", "CHAVE_ESPECIFICA", "CHAVE_GERAL", "pasta"]

    root = os.path.join(os.getcwd(), "media_mailing_cpf", "viabilidades")
    tipos_viabilidade = ["Primaria_", "Secundaria_"]
    for estado in estados:
        salva_status(nova_execucao, f"Iniciando validação dos dados de viabilidades no estado {estado}",  status="Em Andamento")

        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}{estado}.csv"
            filepath = os.path.join(root,file)
            if not os.path.exists(filepath):
                continue
            arquivo = Path(filepath)
            timestamp = arquivo.stat().st_ctime
            data_criacao = datetime.fromtimestamp(timestamp)

            agora = datetime.now()

            if agora - data_criacao > timedelta(hours=24):
                # arquivo não foi criado nas últimas 24h
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} não foi criado hoje.",status="Erro")
                return False
            
            #verificar se todos os estados possuem as mesmas colunas
            df = pd.read_csv(filepath, sep=";")
            if not all([col in df.columns.tolist() for col in COLUNAS_CPF]):
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} não possui as colunas esperadas",status="Erro")            
                return False
    
    return True

def verificador_fase_2(sistema, nova_execucao):
    estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    #verificar todos os estados foram atualizados na data atual
    sistemas_dict = {
        "oi": "media",
        "giga_mais": "media_giga_mais",
        "janeiro_2026": "media_janeiro_2026",
        "mailing_cpfs": "media_mailing_cpf",
    }

    root = os.path.join(os.getcwd(), sistemas_dict[sistema], "viabilidades")
    colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3"]
    cnpjs_encontrados = []
    telefones_encontrados = []

    if sistema == "giga_mais":
        tipos_viabilidade = ["Primaria_",]
    else:
        tipos_viabilidade = ["Primaria_", "Secundaria_"]

    salva_status(nova_execucao, f"Iniciando validação dos dados de viabilidades",  status="Em Andamento")

    for estado in estados:
        salva_status(nova_execucao, f"Iniciando validação dos dados de viabilidades no estado {estado}",  status="Em Andamento")

        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}{estado}.csv"
            filepath = os.path.join(root,file)
            arquivo = Path(filepath)
            timestamp = arquivo.stat().st_ctime
            data_criacao = datetime.fromtimestamp(timestamp)

            agora = datetime.now()

            if agora - data_criacao > timedelta(hours=24):
                # arquivo não foi criado nas últimas 24h
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} não foi criado hoje.",status="Erro")
                return False
            
            #verificar se todos os estados possuem as mesmas colunas
            df = pd.read_csv(filepath, sep=";")
            if not all([col in df.columns.tolist() for col in colunas_esperadas]):
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} não possui as colunas esperadas",status="Erro")            
                return False
            
            #verificar se há cnpjs repetidos
            if len(df["cnpj"].tolist()) != len(df["cnpj"].unique().tolist()):
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} possui cnpjs repetidos",status="Erro")            

                return False
            
            df_repetidos = df[df["cnpj"].isin(cnpjs_encontrados)]
            if len(df_repetidos.index) > 1:
                salva_status(nova_execucao, titulo=f"Erro verificar cnpjs com viabilidade. Arquivo {file} possui cnpjs repetidos com outro arquivo",status="Erro")            

                return False
            
            cnpjs_encontrados += df["cnpj"].unique().tolist()

            # colunas_telefone = ["TEL1", "TEL2", "TEL3"]
            # df_telefones = df[colunas_telefone]
            # for index, row in df_telefones.iterrows():
            #     tels = [row["TEL1"], row["TEL2"], row["TEL3"]]

    return True