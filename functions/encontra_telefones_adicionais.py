import os, re
from .contantes import *
from data.models import *
import pandas as pd
from .utils import clean_phone_number
from pathlib import Path
from datetime import datetime, timedelta


def fase_4_enriquecer(sistema, nova_execucao):
    raiz = os.path.join(os.getcwd(), PASTAS_RAIZ[sistema])
    viabilidades_credito_path = os.path.join(raiz, "viabilidades_credito")
    viabilidades_credito_enriquecido_path = os.path.join(raiz, "viabilidades_credito_enriquecido")
    for file in os.listdir(viabilidades_credito_enriquecido_path):
        os.remove(os.path.join(viabilidades_credito_enriquecido_path, file))
    enriquecimento_path = os.path.join(os.getcwd(), "media", "arquivos_enriquecimento", "enriquecimento.csv")

    df_enriquecimento = pd.read_csv(enriquecimento_path, sep=";", dtype=str,)
    df_enriquecimento["DOCUMENTO"] = df_enriquecimento["DOCUMENTO"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
    todos_telefones = set()
    for file in os.listdir(viabilidades_credito_path):
        filepath = os.path.join(viabilidades_credito_path, file)
        
        estado = filepath.split(".")[0].split("_")[-1]
        tipo_viabilidade = filepath.split(".")[0].split("_")[-2]
        salva_status(nova_execucao, titulo=f"Encontrando telefones adicionais para os cnpjs do tipo {tipo_viabilidade} no estado {estado} ",status="Em Andamento")            

        df_viabilidades_credito = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)
        df_viabilidades_credito["cnpj"] = df_viabilidades_credito["cnpj"].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(14))
        

        cols_telefone = [f"Telefone_{i}" for i in range(1,21)]

        df_viabilidades_credito = df_viabilidades_credito.merge(
            df_enriquecimento[["DOCUMENTO"] + cols_telefone],
            left_on="cnpj",
            right_on="DOCUMENTO",
            how="left"
        )
        cols_grupo1 = ["TEL1", "TEL2", "TEL3"]
        cols_grupo2 = cols_telefone

        cols_telefones = cols_grupo1 + cols_grupo2

        df_viabilidades_credito[cols_telefones] = (
            df_viabilidades_credito[cols_telefones].apply(lambda col: col.map(clean_phone_number))
        )

        def ordenar_telefones(row, cols):
            tels = list(set([t for t in row[cols] if t and t not in todos_telefones]))   # mantém só telefones válidos
            tels += [""] * (len(cols) - len(tels))  # completa com vazio
            row[cols] = tels
            todos_telefones.update(tels)
            return row

        df_viabilidades_credito = df_viabilidades_credito.apply(
            ordenar_telefones, axis=1, cols=cols_grupo1
        )

        df_viabilidades_credito = df_viabilidades_credito.apply(
            ordenar_telefones, axis=1, cols=cols_grupo2
        )

        df_viabilidades_credito.drop(columns=["DOCUMENTO", "CHAVE_ESPECIFICA", "CHAVE_GERAL"], inplace=True)

        df_viabilidades_credito.to_csv(os.path.join(viabilidades_credito_enriquecido_path, file), sep=";", index=False)
    
    if verificador_fase_4(sistema, nova_execucao):
        salva_status(nova_execucao, "Enriquecimento de telefones concluído.", status="Concluido")
        return True


def verificador_fase_4(sistema, nova_execucao):
    #verificar todos os estados foram atualizados na data atual
    sistemas_dict = {
        "oi": "media",
        "giga_mais": "media_giga_mais",
        "janeiro_2026": "media_janeiro_2026"
    }

    root = os.path.join(os.getcwd(), sistemas_dict[sistema], "viabilidades_credito_enriquecido")
    cols_telefone = [f"Telefone_{i}" for i in range(1,21)]
    colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3", "credito"] + cols_telefone
    cnpjs_encontrados = []
    telefones_encontrados = []

    tipos_viabilidade = ["Primaria", "Secundaria"]
    for estado in ESTADOS_BR:
        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}_{estado}.csv"
            filepath = os.path.join(root,file)
            if not os.path.exists(filepath):
                salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} não existe.",status="Erro")
                return False
            arquivo = Path(filepath)
            timestamp = arquivo.stat().st_ctime
            data_criacao = datetime.fromtimestamp(timestamp)

            agora = datetime.now()

            if agora - data_criacao > timedelta(hours=24):
                # arquivo não foi criado nas últimas 24h
                salva_status(
                    nova_execucao,
                    titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} não foi criado nas últimas 24h.",
                    status="Erro"
                )
                return False
            
            #verificar se todos os estados possuem as colunas esperadas
            df = pd.read_csv(filepath, sep=";")
            if not all([col in df.columns.tolist() for col in colunas_esperadas]):
                salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} não possui as colunas esperadas",status="Erro")            
                return False
            
            #verificar se há cnpjs repetidos
            if len(df["cnpj"].tolist()) != len(df["cnpj"].unique().tolist()):
                salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} possui cnpjs repetidos",status="Erro")            

                return False
            
            df_repetidos = df[df["cnpj"].isin(telefones_encontrados)]
            if len(df_repetidos.index) > 1:
                salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} possui cnpjs repetidos com outro arquivo",status="Erro")            

                return False
            

            for col_tel in cols_telefone:
                df_repetidos = df[df[col_tel].isin(telefones_encontrados)]
                if len(df_repetidos.index) > 1:
                    salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} possui telefones repetidos com outro arquivo",status="Erro")            

                    return False
                
                telefones_encontrados += df[col_tel].unique().tolist()
            

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