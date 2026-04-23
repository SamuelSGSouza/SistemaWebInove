import pandas as pd
from data.models import *
import os, traceback, re
from functions.contantes import *
from pathlib import Path
from datetime import datetime, timedelta

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

def pega_lote(string) ->str:
    padrao = r'\bLOTE\s*\w+'
    match = re.search(padrao, string)
    if match:
        lote = match.group()
        lote = re.sub(r"\s*", "", lote).strip().replace("LOTE", "LT").replace("nan", "")
        return lote
    return ""

def fase_2_concatenador(sistema, nova_execucao:Status_Execucoe_DB):
    pasta_receita_federal = os.path.join(os.getcwd(), "media", "arquivos_receita_federal")
    salva_status(nova_execucao, f"Iniciando análise de viabilidades para o sistema {sistema}", status="Em Andamento")
    if sistema == "oi":
        try:
            # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
            dtype={"HP_LIVRE": int, "CEP": "string"}
            path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
            path_viabilidades = os.path.join(os.getcwd(), "media", "viabilidades")

            for file in os.listdir(path_viabilidades):
                os.remove(os.path.join(path_viabilidades, file))

            for estado in ESTADOS_BR:        
                salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
    
                df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
                df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")
                df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))

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

                ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

                df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]

                df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
                padrao = r'\b(apto|apartamento|sala|bloco)\b'
                df_receita_mailing_secundario = df_receita_mailing_secundario[
                    ~df_receita_mailing_secundario['complemento1']
                    .fillna('')
                    .str.contains(padrao, case=False, regex=True)
                ]
                
                df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)


            
                    

        except Exception as e:
            salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} não possui as colunas esperadas",status="Erro")            
            return False

    elif sistema == "giga_mais":
        dtype={"CEP": "string"}
        path_arquivos_dfv = os.path.join(os.getcwd(), "media_giga_mais", "arquivos_dfv")
        path_viabilidades = os.path.join(os.getcwd(), "media_giga_mais", "viabilidades")

        for file in os.listdir(path_viabilidades):
            os.remove(os.path.join(path_viabilidades, file))

        dfs_dfv = []
        for file in os.listdir(path_arquivos_dfv):
            df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
            dfs_dfv.append(df_dfv_estado)
        df_dfv = pd.concat(dfs_dfv)

        df_dfv["cep_geral"] = df_dfv["CEP"].apply(lambda x: str(x).endswith("000"))
        df_dfv = df_dfv[df_dfv["cep_geral"] != True]

        ceps_permitidos = df_dfv["CEP"].unique().tolist()
        for estado in ESTADOS_BR:        
            salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")

            

            df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
            df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")
            df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))

            df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

            

            

            


            df_receita_viaveis = df_receita[df_receita["cep"].isin(ceps_permitidos)]
            padrao = r'\b(apto|apartamento|sala|bloco)\b'
            df_receita_viaveis = df_receita_viaveis[
                    ~df_receita_viaveis['complemento1']
                    .fillna('')
                    .str.contains(padrao, case=False, regex=True)
                ]
            df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)

    elif sistema == "janeiro_2026":
        try:
            
            cnpjs_ja_coletados = []
            pasta_cnpjs_coletados = os.path.join(os.getcwd(), "media", "viabilidades")
            for file in os.listdir(pasta_cnpjs_coletados):
                filepath = os.path.join(pasta_cnpjs_coletados, file)

                df_coletado = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)
                df_coletado["cnpj"] = df_coletado["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
                cnpjs = df_coletado["cnpj"].unique().tolist()
                cnpjs_ja_coletados+= cnpjs

            # COLUNAS_DFV=["UF","MUNICIPIO","LOCALIDADE","BAIRRO","LOGRADOURO","CEP","CELULA","TIPO_CDO","COMPLEMENTO2","COMPLEMENTO3","CODIGO_LOGRADOURO","NO_FACHADA","COMPLEMENTO1","VIABILIDADE_ATUAL","HP_TOTAL","HP_LIVRE","OPB_CEL","DT_ATUALIZACAO"]
            dtype={"CEP": "string", "FACHADA": "string", "ENDERECO":"string"}
            path_arquivos_dfv = os.path.join(os.getcwd(), "media_janeiro_2026", "arquivos_dfv")
            path_viabilidades = os.path.join(os.getcwd(), "media_janeiro_2026", "viabilidades")

            for file in os.listdir(path_viabilidades):
                os.remove(os.path.join(path_viabilidades, file))

            for estado in ESTADOS_BR:        
                salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
    
                df_receita = pd.read_csv(os.path.join(pasta_receita_federal, f"{estado}.csv"), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
                df_receita["cnpj"] = df_receita["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
                df_receita = gera_campos_cep(df_receita, "cep", "num_fachada", "logradouro")

                df_receita.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)

                dfs_dfv = []
                for file in os.listdir(path_arquivos_dfv):
                    if estado in file:

                        df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
                        dfs_dfv.append(df_dfv_estado)
                
                if len(dfs_dfv) < 1:
                    salva_status(nova_execucao, f"Nenhum dfv encontrado para o estado {estado}", status="Erro")
                    return False

                df_dfv = pd.concat(dfs_dfv)

                df_dfv = gera_campos_cep(df_dfv, "CEP", "FACHADA", "ENDERECO")

                chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
                chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

                chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
                chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

                df_receita_cep_especifico = df_receita[df_receita["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
                df_receita_cep_geral = df_receita[df_receita["CHAVE_GERAL"].isin(chaves_geral_dfv)]


                df_receita_viaveis:pd.DataFrame = pd.concat([df_receita_cep_especifico, df_receita_cep_geral])

                df_receita_viaveis.drop_duplicates(subset=["cnpj"], keep="first", inplace=True)
                df_receita_viaveis.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv"), sep=";", index=False)

                ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

                df_receita_nao_coletados = df_receita[~df_receita["cnpj"].isin(df_receita_viaveis["cnpj"].unique().tolist())]

                df_receita_mailing_secundario = df_receita_nao_coletados[df_receita_nao_coletados["cep"].isin(ceps_especificos_dfv)]
                padrao = r'\b(apto|apartamento|sala|bloco)\b'

                df_receita_mailing_secundario = df_receita_mailing_secundario[
                    ~df_receita_mailing_secundario['complemento1']
                    .fillna('')
                    .str.contains(padrao, case=False, regex=True)
                ]
                
                df_receita_mailing_secundario.to_csv(os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv"), sep=";", index=False)




            
                    

        except Exception as e:
            print(traceback.format_exc())
            salva_status(nova_execucao, titulo=f"{e}",status="Erro")            
            return False

    elif sistema == "mailing_cpfs":
        COLUNAS_CPF=["cpf", "nome", "endereco", "numero", "complemento","cep", "bairro","cidade", "uf", "fixo_1", "fixo_2", "fixo_3", "celular_1", "celular_2", "celular_3", "renda_presumida"]
        paths_arquivos_cpf = [
            os.path.join(os.getcwd(), "media_mailing_cpf", "arquivos_cpf_externo"),
        ]
        path_arquivos_dfv = os.path.join(os.getcwd(), "media", "arquivos_dfv")
        path_viabilidades = os.path.join(os.getcwd(), "media_mailing_cpf", "viabilidades")
        os.makedirs(path_viabilidades, exist_ok=True)
        dtype={"HP_LIVRE": int, "CEP": "string"}
        for file in os.listdir(path_viabilidades):
            os.remove(os.path.join(path_viabilidades, file))
    
        for estado in ESTADOS_BR:        
            salva_status(nova_execucao, f"Iniciando análise de viabilidades no estado {estado}", status="Em Andamento")
            
            dfs_dfv = []
            for file in os.listdir(path_arquivos_dfv):
                if estado in file:

                    df_dfv_estado = pd.read_excel(os.path.join(path_arquivos_dfv, file), dtype=dtype)
                    dfs_dfv.append(df_dfv_estado)

            df_dfv = pd.concat(dfs_dfv)
            df_dfv = df_dfv[df_dfv["HP_LIVRE"] >= 1]
            df_dfv = gera_campos_cep(df_dfv, "CEP", "NO_FACHADA", "LOGRADOURO")

            chaves_especificas_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_ESPECIFICA"].unique().tolist()
            chaves_especificas_dfv = [c for c in chaves_especificas_dfv if len(c)>4]

            chaves_geral_dfv = df_dfv[df_dfv["CEP"].astype(str).str.endswith("000")]["CHAVE_GERAL"].unique().tolist()
            chaves_geral_dfv = [c for c in chaves_geral_dfv if len(c)>4]

            dfs_receita = []
            for pasta in paths_arquivos_cpf:
                os.makedirs(pasta, exist_ok=True)
                for file in os.listdir(pasta):
                    if estado in file:

                        chunks = pd.read_csv(os.path.join(pasta,file), sep=";", dtype=str, chunksize=1_000_000)
                        for df_cpf in chunks:
                            df_cpf.columns = df_cpf.columns.str.lower()
                            df_cpf.rename(columns={
                                    "tel_fixo1": "fixo_1",
                                    "fixo1": "fixo_1",
                                    "tel_fixo2": "fixo_2",
                                    "fixo2": "fixo_2",
                                    "tel_fixo3": "fixo_3",
                                    "fixo3": "fixo_3",
                                    "celular1": "celular_1",
                                    "celular2": "celular_2",
                                    "celular3": "celular_3",
                                    "celular3": "celular_3",
                                    "renda pressumida": "renda_pressumida",

                                }, inplace=True)
                            df_cpf = df_cpf[COLUNAS_CPF]
                            df_cpf = gera_campos_cep(df_cpf, "cep", "numero", "endereco")
                            df_cpf["cpf"] = df_cpf["cpf"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(11))
                            df_cpf.drop_duplicates(subset=["cpf"], keep="first", inplace=True)
                            
                            df_cpf["pasta"] = pasta


                            df_cpf_estado_cep_especifico = df_cpf[df_cpf["CHAVE_ESPECIFICA"].isin(chaves_especificas_dfv)]
                            df_cpf_estado_cep_geral = df_cpf[df_cpf["CHAVE_GERAL"].isin(chaves_geral_dfv)]


                            df_cpf_estado_viaveis:pd.DataFrame = pd.concat([df_cpf_estado_cep_especifico, df_cpf_estado_cep_geral])

                            
                            nome_arquivo = os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv")
                            write_header = not os.path.exists(nome_arquivo)
                            df_cpf_estado_viaveis.to_csv(nome_arquivo, mode="a", header=write_header, index=False, sep=";", encoding="utf-8")



                            ceps_especificos_dfv = df_dfv[~df_dfv["CEP"].astype(str).str.endswith("000")]["CEP"].unique().tolist()

                            df_cpf_estado_nao_coletados = df_cpf[~df_cpf["CHAVE_ESPECIFICA"].isin(df_cpf_estado_viaveis["CHAVE_ESPECIFICA"].unique().tolist())]

                            df_cpf_estado_mailing_secundario = df_cpf_estado_nao_coletados[df_cpf_estado_nao_coletados["cep"].isin(ceps_especificos_dfv)]
                            padrao = r'\b(apto|apartamento|sala|bloco)\b'
                            df_cpf_estado_mailing_secundario = df_cpf_estado_mailing_secundario[
                                ~df_cpf_estado_mailing_secundario['complemento']
                                .fillna('')
                                .str.contains(padrao, case=False, regex=True)
                            ]

                            nome_arquivo = os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv")
                            write_header = not os.path.exists(nome_arquivo)
                            df_cpf_estado_mailing_secundario.to_csv(nome_arquivo, mode="a", header=write_header, index=False, sep=";", encoding="utf-8")


            nome_arquivo_primario = os.path.join(path_viabilidades, f"Viabilidade_Primaria_{estado}.csv")
            colunas_esperadas = COLUNAS_CPF + ["CHAVE_ESPECIFICA", "CHAVE_GERAL", "pasta"]
            texto_colunas_esperadas = ";".join(colunas_esperadas)
            if not os.path.exists(nome_arquivo_primario):
                with open(nome_arquivo_primario, "w", encoding="utf-8") as arq:
                    arq.write(texto_colunas_esperadas)

            nome_arquivo_secundario = os.path.join(path_viabilidades, f"Viabilidade_Secundaria_{estado}.csv")
            if not os.path.exists(nome_arquivo_secundario):
                with open(nome_arquivo_secundario, "w", encoding="utf-8") as arq:
                    arq.write(texto_colunas_esperadas)

            
                        
        return verificador_fase_2_cpf(sistema, nova_execucao)
         

            

    return verificador_fase_2(sistema, nova_execucao)

def verificador_fase_2_cpf(sistema, nova_execucao):
    estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    COLUNAS_CPF=["cpf", "nome", "endereco", "numero", "complemento","cep", "bairro","cidade", "uf", "fixo_1", "fixo_2", "fixo_3", "celular_1", "celular_2", "celular_3", "renda_presumida", "CHAVE_ESPECIFICA", "CHAVE_GERAL", "pasta"]

    root = os.path.join(os.getcwd(), "media_mailing_cpf", "viabilidades")
    tipos_viabilidade = ["Primaria_", "Secundaria_"]
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