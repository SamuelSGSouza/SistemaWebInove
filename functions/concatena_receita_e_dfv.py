import pandas as pd
from data.models import *
import os, traceback, re
from functions.contantes import *

def gera_campos_cep(df:pd.DataFrame, campo_cep, campo_numero, campo_logradouro)-> pd.DataFrame:
    df[campo_numero] = df[campo_numero].apply(lambda x: re.sub(r'\D', '', str(x))) #tirando letras do número

    df["CHAVE_ESPECIFICA"] = df[campo_cep].astype(str) + df[campo_numero].astype(str)
    df["CHAVE_GERAL"] = df[campo_cep].astype(str) + df[campo_logradouro].astype(str).str[-3:] + df[campo_numero].astype(str)

    for index, row in df.iterrows():
        if str(row[campo_cep]).endswith("000") or len(row["CHAVE_ESPECIFICA"]) < 10:
            df.at[index, "CHAVE_ESPECIFICA"] = ""
        
        if not str(row[campo_cep]).endswith("000") or len(str(row["CHAVE_GERAL"])) < 10:
            df.at[index, "CHAVE_GERAL"] = ""

    

    return df

def pega_lote(string) ->str:
    padrao = r'\bLOTE\s*\w+'
    match = re.search(padrao, string)
    if match:
        lote = match.group()
        lote = re.sub(r"\s*", "", lote).strip().replace("LOTE", "LT").replace("nan", "")
        return lote
    return ""

def fase_2_concatenador(sistema):
    pasta_receita_federal = os.path.join(os.getcwd(), "media", "arquivos_receita_federal")
    if sistema == "oi":
        print("iniciando sistema OI")
        try:
            # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
            dtype={"HP_LIVRE": int, "CEP": "string"}
            path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
            path_viabilidades = os.path.join(os.getcwd(), "media", "viabilidades")

            for file in os.listdir(path_viabilidades):
                os.remove(os.path.join(path_viabilidades, file))

            for estado in ESTADOS_BR:            
                df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
                df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")

                df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

                dfs_dfv = []
                for file in os.listdir(path_arquivos_dfv):
                    if estado in file:

                        df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
                        dfs_dfv.append(df_dfv_estado)

                df_dfv = pd.concat(dfs_dfv)

                df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1]



                df_dfv = gera_campos_cep(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO")

                dfv_mailings_viaveis = []
                if estado in ["DF", "GO"]:
                    campo_complemento_dfv = "COMPLEMENTO1" if estado == "DF" else "COMPLEMENTO2"

                    df_receita["lote"] = df_receita["complemento1"].apply(lambda x: pega_lote(str(x)))
                    df_receita["CHAVE_ESPECIFICA"] = df_receita["cep"] + df_receita["lote"]

                    
                    df_dfv["CHAVE_ESPECIFICA"] = df_dfv["CEP"] + df_dfv[campo_complemento_dfv].astype(str).str.replace(" ", "").replace("nan", "")
                    chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
                    chaves_especificas_dfv = [str(c) for c in chaves_especificas_dfv if len(str(c))>9]

                    df_chaves_lote_df = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]

                    
                    
                    df_receita["numero_tratado"] = df_receita["num_fachada"].apply(lambda x: re.sub(r'\D', '', str(x)))
                    df_receita["CHAVE_GERAL"] = df_receita["cep"] + df_receita["logradouro"].astype(str).str.replace(" ", "").str[-3:] + df_receita["numero_tratado"]
                    
                    df_dfv["numero_tratado"] = df_dfv[campo_complemento_dfv].apply(lambda x: re.sub(r'\D', '', str(x)))
                    df_dfv["CHAVE_GERAL"] = df_dfv["CEP"] + df_dfv["LOGRADOURO"].astype(str).str.replace(" ", "").str[-3:] + df_dfv["numero_tratado"]
                    
                    chaves_gerais_dfv = df_dfv["CHAVE_GERAL"].unique().tolist()
                    chaves_gerais_dfv = [str(c) for c in chaves_gerais_dfv if len(str(c))>9]

                    df_chaves_gerais = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_gerais_dfv)]


                    

                    dfv_mailings_viaveis.append(df_chaves_lote_df)
                    dfv_mailings_viaveis.append(df_chaves_gerais)

                    


                else:

                    chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
                    chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

                    chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
                    chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

                    df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
                    df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]

                    dfv_mailings_viaveis.append(df_receita_cep_especifico)
                    dfv_mailings_viaveis.append(df_receita_cep_geral)


                df_receita_viaveis:pd.DataFrame = pd.concat(dfv_mailings_viaveis)

                df_receita_viaveis.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)
                df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)
                salva_dado(f"Quantidade de Empresas com Viabilidade Primária no Estado {estado}", len(df_receita_viaveis.index))

                ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

                df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]

                df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
                df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)

                salva_dado(f"Quantidade de Empresas com Viabilidade Secundária no Estado {estado}", len(df_receita_mailing_secundario.index))


        except Exception as e:
            print(traceback.format_exc())
