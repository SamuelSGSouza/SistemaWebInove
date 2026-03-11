import pandas as pd
import os, re
from .contantes import *
from data.models import *
from pathlib import Path
from datetime import datetime
from .contantes import *

def fase_3_define_credito(nova_execucao:Status_Execucoe_DB, sistema):
    raiz = os.path.join(os.getcwd(), PASTAS_RAIZ[sistema])
    viabilidades_path = os.path.join(raiz, "viabilidades")
    viabilidades_credito_path = os.path.join(raiz, "viabilidades_credito")
    for file in os.listdir(viabilidades_credito_path):
        os.remove(os.path.join(viabilidades_credito_path, file))
    creditos_path = os.path.join(os.getcwd(), "media", "arquivos_credito", "credito.csv")

    df_credito = pd.read_csv(creditos_path, sep=";",)
    df_credito["CNPJ"] = df_credito["CNPJ"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
    df_credito["CONTAINS_K"] = df_credito["LETRAS_STATUS"].apply(lambda x: True if "k" in str(x).lower() else False)
    df_credito = df_credito[df_credito["CONTAINS_K"] == False]

    cnpjs_aprovados = df_credito[df_credito["APROVADO/NEGADO"] == "S"]["CNPJ"].unique().tolist()
    cnpjs_negados = df_credito[df_credito["APROVADO/NEGADO"] != "S"]["CNPJ"].unique().tolist()

    for file in os.listdir(viabilidades_path):
        filepath = os.path.join(viabilidades_path, file)

        estado = filepath.split(".")[0].split("_")[-1]
        tipo_viabilidade = filepath.split(".")[0].split("_")[-2]

        salva_status(nova_execucao, f"Análise de crédito nos cnpjs com viabilidade {tipo_viabilidade} no estado {estado}", status="Em Andamento")

        df_viabilidade = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)
        df_viabilidade["cnpj"] = df_viabilidade["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
        df_viabilidade["credito"] = "Sem Infos"

        df_viabilidade.loc[
            df_viabilidade["cnpj"].isin(cnpjs_aprovados),
            "credito"
        ] = "Aprovado"

        df_viabilidade.loc[
            df_viabilidade["cnpj"].isin(cnpjs_negados),
            "credito"
        ] = "Negado"
        
        

        df_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] == "S"]
        df_N_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] != "S"]
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - MEI", 
            len(df_meis[df_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist())
        )
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - NAO MEI", 
            len(df_N_meis[df_N_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist())
        )
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - MEI", 
            len(df_meis[df_meis["credito"] == "Negado"]["cnpj"].unique().tolist())
        )
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - NAO MEI", 
            len(df_N_meis[df_N_meis["credito"] == "Negado"]["cnpj"].unique().tolist())
        )

        df_viabilidade.to_csv(os.path.join(viabilidades_credito_path,file ), sep=";", index=False)



    if verificador_fase_3(sistema, nova_execucao):
        salva_status(nova_execucao, f"Análises de crédito finalizadas", status="Concluido")
        return True

def verificador_fase_3(sistema, nova_execucao):
    #verificar todos os estados foram atualizados na data atual
    sistemas_dict = {
        "oi": "media",
        "giga_mais": "media_giga_mais",
        "janeiro_2026": "media_janeiro_2026"
    }

    root = os.path.join(os.getcwd(), sistemas_dict[sistema], "viabilidades_credito")
    colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3", "credito"]
    cnpjs_encontrados = []
    telefones_encontrados = []

    tipos_viabilidade = ["Primaria", "Secundaria"]
    for estado in ESTADOS_BR:
        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}_{estado}.csv"
            filepath = os.path.join(root,file)
            if not os.path.exists(filepath):
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} não existe.",status="Erro")
                return False
            arquivo = Path(filepath)
            timestamp = arquivo.stat().st_ctime
            data = datetime.fromtimestamp(timestamp)
            hoje = datetime.today()
            if hoje.day != data.day or hoje.month != data.month:
                #data de criação não foi hoje
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} não foi criado hoje.",status="Erro")
                return False
            
            #verificar se todos os estados possuem as colunas esperadas
            df = pd.read_csv(filepath, sep=";")
            if not all([col in df.columns.tolist() for col in colunas_esperadas]):
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} não possui as colunas esperadas",status="Erro")            
                return False
            
            #verificar se há cnpjs repetidos
            if len(df["cnpj"].tolist()) != len(df["cnpj"].unique().tolist()):
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} possui cnpjs repetidos",status="Erro")            

                return False
            
            df_repetidos = df[df["cnpj"].isin(cnpjs_encontrados)]
            if len(df_repetidos.index) > 1:
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} possui cnpjs repetidos com outro arquivo",status="Erro")            

                return False
            
            

            tipos_credito = df["credito"].unique().tolist()
            if len(tipos_credito) != 3:
                salva_status(nova_execucao, titulo=f"Erro verificar análise de crédito. Arquivo {file} possui tipos de crédito inválidos: {tipos_credito}",status="Erro")            
                return False

            cnpjs_encontrados += df["cnpj"].unique().tolist()

            # colunas_telefone = ["TEL1", "TEL2", "TEL3"]
            # df_telefones = df[colunas_telefone]
            # for index, row in df_telefones.iterrows():
            #     tels = [row["TEL1"], row["TEL2"], row["TEL3"]]

    return True