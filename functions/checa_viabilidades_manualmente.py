import os
import pandas as pd
import re, chardet
import csv

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

# Se este arquivo ficar em outro módulo, importe a função original:
# from functions.fase_2 import gera_campos_cep


VIABILIDADE_PRIMARIA = "VIABILIDADE PRIMÁRIA"
VIABILIDADE_SECUNDARIA = "VIABILIDADE SECUNDÁRIA"
NAO_VIAVEL = "NÃO VIÁVEL"

PADRAO_COMPLEMENTO_BLOQUEADO = r"\b(apto|apartamento|sala|bloco)\b"

EXTENSOES_CSV = (".csv", ".txt")
EXTENSOES_EXCEL = (".xls", ".xlsx", ".xlsb", ".xlsm")

def gera_campos_cep(df:pd.DataFrame, campo_cep, campo_numero, campo_logradouro)-> pd.DataFrame:
    df[campo_numero] = df[campo_numero].apply(lambda x: re.sub(r'\D', '', str(x))) #tirando letras do número

    df["CHAVE_ESPECIFICA"] = df[campo_cep].astype(str) + df[campo_numero].astype(str)
    df["CHAVE_GERAL"] = df[campo_cep].astype(str) + df[campo_logradouro].astype(str).str[-3:] + df[campo_numero].astype(str)

    for index, row in df.iterrows():
        if str(row[campo_cep]).endswith("000") or len(row["CHAVE_ESPECIFICA"].strip()) < 9:
            df.at[index, "CHAVE_ESPECIFICA"] = ""
        
        if not str(row[campo_cep]).endswith("000") or len(str(row["CHAVE_GERAL"])) < 10:
            df.at[index, "CHAVE_GERAL"] = ""

    

    return df

def _ler_planilha(caminho: str, dtype=str) -> pd.DataFrame:
    """Lê csv/txt/xls/xlsx detectando separador e encoding quando necessário."""
    ext = os.path.splitext(caminho)[1].lower()

    if ext in EXTENSOES_CSV:
        return pd.read_csv(
            caminho,
            sep=_detectar_sep_csv(caminho),
            encoding=_detectar_encoding_csv(caminho),
            dtype=dtype,
            on_bad_lines="skip",
        )

    if ext in EXTENSOES_EXCEL:
        return pd.read_excel(caminho, dtype=dtype)

    raise ValueError(f"Formato de arquivo não suportado: {caminho}")


def _ler_dfv(caminho_dfv: str) -> pd.DataFrame:
    """Aceita tanto o caminho de um arquivo de DFV quanto de uma pasta com vários."""
    if os.path.isdir(caminho_dfv):
        arquivos = [
            os.path.join(caminho_dfv, f)
            for f in sorted(os.listdir(caminho_dfv))
            if os.path.splitext(f)[1].lower() in EXTENSOES_CSV + EXTENSOES_EXCEL
        ]
    else:
        arquivos = [caminho_dfv]

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo de DFV encontrado em: {caminho_dfv}")

    return pd.concat([_ler_planilha(arq) for arq in arquivos], ignore_index=True)


def classifica_viabilidade_df(
    df: pd.DataFrame,
    df_dfv: pd.DataFrame,
    campo_cep: str = "cep",
    campo_numero: str = "num_fachada",
    campo_logradouro: str = "logradouro",
    campo_complemento: str = "complemento1",
    campo_cep_dfv: str = "CEP",
    campo_numero_dfv: str = "NO_FACHADA",
    campo_logradouro_dfv: str = "LOGRADOURO",
    coluna_saida: str = "VIABILIDADE",
    manter_chaves: bool = False,
    filtrar_hp_livre: bool = True,
) -> pd.DataFrame:
    """
    Classifica cada linha do mailing sem remover nenhum registro.

    Regras (mesmas da fase_2_concatenador):
      - VIABILIDADE PRIMÁRIA .... CHAVE_ESPECIFICA bate com um CEP específico do DFV
                                  OU CHAVE_GERAL bate com um CEP geral (terminado em 000) do DFV.
      - VIABILIDADE SECUNDÁRIA .. não é primária, mas o CEP está entre os CEPs específicos
                                  do DFV e o complemento não contém apto/apartamento/sala/bloco.
      - NÃO VIÁVEL .............. nenhum dos casos acima.
    """
    df = df.copy()

    # mesmo filtro das outras fases: só endereços com porta livre
    if filtrar_hp_livre and "HP_LIVRE" in df_dfv.columns:
        hp_livre = pd.to_numeric(df_dfv["HP_LIVRE"], errors="coerce").fillna(0)
        df_dfv = df_dfv[hp_livre >= 1]

    # ---------- chaves do DFV ----------
    df_dfv = gera_campos_cep(
        df_dfv.copy(), campo_cep_dfv, campo_numero_dfv, campo_logradouro_dfv
    )

    cep_dfv = df_dfv[campo_cep_dfv].astype(str).str.strip()
    mask_cep_especifico = ~cep_dfv.str.endswith("000")

    chaves_especificas_dfv = {
        c
        for c in df_dfv.loc[mask_cep_especifico, "CHAVE_ESPECIFICA"].astype(str)
        if len(c) > 4
    }
    chaves_gerais_dfv = {
        c
        for c in df_dfv.loc[~mask_cep_especifico, "CHAVE_GERAL"].astype(str)
        if len(c) > 4
    }
    ceps_especificos_dfv = set(cep_dfv[mask_cep_especifico])

    # ---------- chaves do mailing ----------
    # gera_campos_cep altera o campo de número in-place, por isso trabalhamos numa cópia
    df_chaves = gera_campos_cep(df.copy(), campo_cep, campo_numero, campo_logradouro)

    chave_especifica = df_chaves["CHAVE_ESPECIFICA"].astype(str)
    chave_geral = df_chaves["CHAVE_GERAL"].astype(str)

    # ---------- classificação ----------
    mask_primaria = chave_especifica.isin(chaves_especificas_dfv) | chave_geral.isin(
        chaves_gerais_dfv
    )

    cep_mailing = df_chaves[campo_cep].astype(str).str.strip()

    if campo_complemento and campo_complemento in df_chaves.columns:
        complemento_valido = ~df_chaves[campo_complemento].fillna("").astype(
            str
        ).str.contains(PADRAO_COMPLEMENTO_BLOQUEADO, case=False, regex=True)
    else:
        complemento_valido = pd.Series(True, index=df_chaves.index)

    mask_secundaria = (
        (~mask_primaria) & cep_mailing.isin(ceps_especificos_dfv) & complemento_valido
    )

    df[coluna_saida] = NAO_VIAVEL
    df.loc[mask_secundaria, coluna_saida] = VIABILIDADE_SECUNDARIA
    df.loc[mask_primaria, coluna_saida] = VIABILIDADE_PRIMARIA

    if manter_chaves:
        df["CHAVE_ESPECIFICA"] = chave_especifica
        df["CHAVE_GERAL"] = chave_geral

    return df


def classifica_viabilidade_arquivo(
    caminho_arquivo: str,
    caminho_dfv: str,
    caminho_saida: str = None,
    campo_cep: str = "cep",
    campo_numero: str = "num_fachada",
    campo_logradouro: str = "logradouro",
    campo_complemento: str = "complemento1",
    campo_cep_dfv: str = "CEP",
    campo_numero_dfv: str = "NO_FACHADA",
    campo_logradouro_dfv: str = "LOGRADOURO",
    coluna_saida: str = "VIABILIDADE",
    manter_chaves: bool = False,
    filtrar_hp_livre: bool = True,
) -> pd.DataFrame:
    """
    Recebe o caminho de um mailing e o caminho do DFV correspondente (arquivo ou pasta),
    devolve o DataFrame original com a coluna de viabilidade adicionada.
    Nenhuma linha é excluída.

    Exemplo:
        df = classifica_viabilidade_arquivo(
            "media/mailings/base_ES.csv",
            "media/arquivos_dfv/DFV_ES.xlsx",
            caminho_saida="media/saida/base_ES_classificada.csv",
        )
        print(df["VIABILIDADE"].value_counts())
    """
    df = _ler_planilha(caminho_arquivo)
    df_dfv = _ler_dfv(caminho_dfv)

    colunas_obrigatorias = [campo_cep, campo_numero, campo_logradouro]
    faltando = [c for c in colunas_obrigatorias if c not in df.columns]
    if faltando:
        raise KeyError(
            f"O arquivo {os.path.basename(caminho_arquivo)} não possui as colunas: {faltando}"
        )

    faltando_dfv = [
        c
        for c in [campo_cep_dfv, campo_numero_dfv, campo_logradouro_dfv]
        if c not in df_dfv.columns
    ]
    if faltando_dfv:
        raise KeyError(f"O DFV não possui as colunas: {faltando_dfv}")

    df = classifica_viabilidade_df(
        df,
        df_dfv,
        campo_cep=campo_cep,
        campo_numero=campo_numero,
        campo_logradouro=campo_logradouro,
        campo_complemento=campo_complemento,
        campo_cep_dfv=campo_cep_dfv,
        campo_numero_dfv=campo_numero_dfv,
        campo_logradouro_dfv=campo_logradouro_dfv,
        coluna_saida=coluna_saida,
        manter_chaves=manter_chaves,
        filtrar_hp_livre=filtrar_hp_livre,
    )

    if caminho_saida:
        pasta_saida = os.path.dirname(os.path.abspath(caminho_saida))
        os.makedirs(pasta_saida, exist_ok=True)
        df.to_csv(caminho_saida, sep=";", index=False, encoding="utf-8")

    return df


if __name__ == "__main__":
    df_tratado = classifica_viabilidade_arquivo("checar_viabilidade_NIO.xlsx", "ES.xlsb")
    df_tratado.to_excel("Viabilidades_Checadas.xlsx", index=False)