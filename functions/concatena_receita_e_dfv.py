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

def fase_2_concatenador(sistema):
    pasta_receita_federal = os.path.join(os.getcwd(), "media", "arquivos_receita_federal")
    if sistema == "oi":
        print("iniciando sistema OI")
        try:
            # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
            dtype={"HP_LIVRE": int, "CEP": "string"}
            path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
            path_viabilidades = os.path.join(os.getcwd(), "media", "viabilidades")

            # for file in os.listdir(path_viabilidades):
            #     os.remove(os.path.join(path_viabilidades, file))

            for estado in ESTADOS_BR:            
                df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
                df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")
                print(f"Carregou arquivo df_receita no estado {estado}")
                print(df_receita["CHAVE_ESPECIFICA"].unique().tolist()[:10])
                dfs_dfv = []
                for file in os.listdir(path_arquivos_dfv):
                    if estado in file:
                        print(f"Pegando arquivo {file}")
                        df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
                        dfs_dfv.append(df_dfv_estado)

                df_dfv = pd.concat(dfs_dfv)
                print(f"Total de possíveis viabilidades no estado {estado} - {len(df_dfv)}")
                df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1]
                print(f"Total de possíveis viabilidades no estado {estado} com HP Livre - {len(df_dfv)}")


                df_dfv = gera_campos_cep(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO")

                dfv_mailings_viaveis = []
                if estado in ["DF", "GO"]:
                    df_dfv["complemento_padrao"] = (
                        df_dfv["COMPLEMENTO1"]
                        .astype(str)
                        .str.replace("LT", "LOTE", regex=False)
                    )
                    print("Substituiu LT por LOTE")
                    # Faz o merge pelo CEP
                    df_merge = df_receita.merge(
                        df_dfv[["CEP", "complemento_padrao"]],
                        left_on="cep",
                        right_on="CEP",
                        how="inner"
                    )
                    print("MERGE NAS BASES")

                    # Marca se encontrou o complemento
                    mask = [
                        str(padrao) in str(comp) 
                        for padrao, comp in zip(df_merge["complemento_padrao"], df_merge["complemento1"])
                    ]

                    # Filtra direto usando a máscara booleana (economiza a criação da coluna "localizado")
                    dfv_mailings_viaveis.append(df_merge[mask])
                    print("Pegou localizadods")

                else:

                    chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
                    chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]
                    print(f"Total de chaves específicas: {len(chaves_especificas_dfv)} - {chaves_especificas_dfv[:3]}")
                    chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
                    chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]
                    print(f"Total de chaves gerais: {len(chaves_geral_dfv)} - {chaves_geral_dfv[:3]}")

                    df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
                    df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]

                    dfv_mailings_viaveis.append(df_receita_cep_especifico)
                    dfv_mailings_viaveis.append(df_receita_cep_geral)

                print(f"Gerou Viabilidades primárias no estado {estado}")
                df_receita_viaveis:pd.DataFrame = pd.concat(dfv_mailings_viaveis)
                df_receita_viaveis.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)
                df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)
                salva_dado(f"Quantidade de Empresas com Viabilidade Primária no Estado {estado}", len(df_receita_viaveis.index))
                print(f"Quantidade de Empresas com Viabilidade Primária no Estado {estado} - ", len(df_receita_viaveis.index))

                ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

                df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]
                print(f"Total de empresas da receita federal que não foram coletadas na viabilidade primária: {len(df_receita_nao_coletados.index)}")
                df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
                df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)
                print(f"Gerou Viabilidades secundarias no estado {estado}")
                salva_dado(f"Quantidade de Empresas com Viabilidade Secundária no Estado {estado}", len(df_receita_mailing_secundario.index))
                print(f"Quantidade de Empresas com Viabilidade Secundária no Estado {estado} - ", len(df_receita_mailing_secundario.index))

        except Exception as e:
            print(traceback.format_exc())
