import os, re
from .contantes import *
from data.models import *
import pandas as pd
from .utils import clean_phone_number
from pathlib import Path
from datetime import datetime, timedelta
import traceback

def fase_4_enriquecer(sistema, nova_execucao):
    salva_status(nova_execucao, titulo=f"Iniciando coleta de enriquecimento de telefones",status="Em Andamento")            

    campo_referencia_documento = "cnpj"
    quantidade_caracteres = 14
    cols_telefone_originais = ["TEL1", "TEL2", "TEL3"]
    dtypes_especifico = DTYPES_RECEITA_FEDERAL
    try:
        raiz = os.path.join(os.getcwd(), PASTAS_RAIZ[sistema])
        if sistema == "mailing_cpfs":
            viabilidades_credito_path = os.path.join(raiz, "viabilidades")
            campo_referencia_documento = "cpf"
            quantidade_caracteres = 11
            cols_telefone_originais = [ "celular_1", "celular_2", "celular_3"]
            dtypes_especifico = str

        elif sistema != "giga_mais":
            viabilidades_credito_path = os.path.join(raiz, "viabilidades_credito")
        else:
            viabilidades_credito_path = os.path.join(raiz, "viabilidades")
        viabilidades_credito_enriquecido_path = os.path.join(raiz, "viabilidades_credito_enriquecido")
        os.makedirs(viabilidades_credito_enriquecido_path, exist_ok=True)
        for file in os.listdir(viabilidades_credito_enriquecido_path):
            os.remove(os.path.join(viabilidades_credito_enriquecido_path, file))
        enriquecimento_path = os.path.join(os.getcwd(), "media", "arquivos_enriquecimento", "enriquecimento.csv")

        df_enriquecimento = pd.read_csv(enriquecimento_path, sep=";", dtype=str,)
        df_enriquecimento["DOCUMENTO"] = df_enriquecimento["DOCUMENTO"].apply(lambda x: re.sub(r"\D+", "", str(x)))
        
        cols_tel = [f"Telefone_{i}" for i in range(1, 21)]

        # transforma telefones em linhas
        df_long = df_enriquecimento.melt(
            id_vars="DOCUMENTO",
            value_vars=cols_tel,
            value_name="telefone"
        )

        # remove vazios

        df_long = df_long.dropna(subset=["telefone"])
        df_long = df_long[df_long["telefone"] != ""]

        # agrupa por documento juntando todos os telefones
        df_group = df_long.groupby("DOCUMENTO")["telefone"].apply(list).reset_index()

        # garante no máximo 20
        df_group["telefone"] = df_group["telefone"].apply(lambda x: x[:20])

        # volta para colunas Telefone_1...Telefone_20
        df_enriquecimento = pd.DataFrame(
            df_group["telefone"].tolist(),
            columns=[f"Telefone_{i}" for i in range(1, 21)]
        )

        df_enriquecimento.insert(0, "DOCUMENTO", df_group["DOCUMENTO"])

        if df_enriquecimento["DOCUMENTO"].duplicated().sum() > 0:
            salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo de enriquecimento possui cnpjs repetidos",status="Erro")            
            return
        
        if len(df_enriquecimento["DOCUMENTO"].unique().tolist()) < 1_000_000:
            salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo de enriquecimento não possui a quantidade de cnpjs esperados",status="Erro")            
            return
        
        if len(df_enriquecimento["Telefone_1"].unique().tolist()) < 1_000_000:
            salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo de enriquecimento não possui a quantidade de telefones esperados",status="Erro")            
            return
        
        todos_telefones = set()
        for file in os.listdir(viabilidades_credito_path):
            estado = str(file).split("_")[-1].split(".")[0].split()

            salva_status(nova_execucao, titulo=f"Iniciando análise no estado {estado}",status="Em Andamento")            

            
            filepath = os.path.join(viabilidades_credito_path, file)
            
            estado = filepath.split(".")[0].split("_")[-1]
            tipo_viabilidade = filepath.split(".")[0].split("_")[-2]
            salva_status(nova_execucao, titulo=f"Encontrando telefones adicionais para os {campo_referencia_documento} do tipo {tipo_viabilidade} no estado {estado} ",status="Em Andamento")            

            df_viabilidades_credito = pd.read_csv(filepath, sep=";", dtype=dtypes_especifico)
            df_viabilidades_credito[campo_referencia_documento] = df_viabilidades_credito[campo_referencia_documento].apply(lambda x: re.sub(r"\D+", "", str(x)).zfill(quantidade_caracteres))
            
            print(f"Total de telefones 1 no ponto 1: {len(df_viabilidades_credito['celular_1'].unique().tolist())}")
            print(df_viabilidades_credito['celular_1'].unique().tolist()[:3])
            cols_telefone = [f"Telefone_{i}" for i in range(1,21)]

            df_viabilidades_credito = df_viabilidades_credito.merge(
                df_enriquecimento[["DOCUMENTO"] + cols_telefone],
                left_on=campo_referencia_documento,
                right_on="DOCUMENTO",
                how="left"
            )
            cols_grupo1 = cols_telefone_originais
            cols_grupo2 = cols_telefone

            cols_telefones = cols_grupo1 + cols_grupo2

            df_viabilidades_credito[cols_telefones] = (
                df_viabilidades_credito[cols_telefones].apply(lambda col: col.map(clean_phone_number))
            )
            print(f"Total de telefones 1 no ponto 2: {len(df_viabilidades_credito['celular_1'].unique().tolist())}")


            def ordenar_telefones(row, cols):
                tels = list(set([t for t in row[cols] if t and t not in todos_telefones]))   # mantém só telefones válidos
                tels += [""] * (len(cols) - len(tels))  # completa com vazio
                row[cols] = tels
                todos_telefones.update(tels)
                return row

            df_viabilidades_credito = df_viabilidades_credito.apply(
                ordenar_telefones, axis=1, cols=cols_grupo1
            )
            print(f"Total de telefones 1 no ponto 3: {len(df_viabilidades_credito['celular_1'].unique().tolist())}")


            df_viabilidades_credito = df_viabilidades_credito.apply(
                ordenar_telefones, axis=1, cols=cols_grupo2
            )
            print(f"Total de telefones 1 no ponto 4: {len(df_viabilidades_credito['celular_1'].unique().tolist())}")


            df_viabilidades_credito.drop(columns=["DOCUMENTO", "CHAVE_ESPECIFICA", "CHAVE_GERAL"], inplace=True)

            if campo_referencia_documento == "cnpj":
                if df_viabilidades_credito["cnpj"].duplicated().sum() > 0:
                    salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Viabilidade do tipo {tipo_viabilidade} no estado {estado} ficou com cnpjs repetidos",status="Erro")            
                    return

            for col in cols_telefone:
                if col not in df_viabilidades_credito.columns.to_list():
                    df_viabilidades_credito[col] = ""

            print(f"Total de telefones 1 no ponto 5: {len(df_viabilidades_credito['celular_1'].unique().tolist())}")


            df_viabilidades_credito.to_csv(os.path.join(viabilidades_credito_enriquecido_path, file), sep=";", index=False)
        
        verificador = verificador_fase_4
        if campo_referencia_documento == "cpf":
            verificador = verificador_fase_4_cpfs

        if verificador(sistema, nova_execucao):
            salva_status(nova_execucao, "Enriquecimento de telefones concluído.", status="Concluido")
            return True
    except Exception as e:
        salva_status(nova_execucao, f"Erro ao enriquecer dados {traceback.format_exc()}", status="Erro")


def verificador_fase_4_cpfs(sistema, nova_execucao):
    root = os.path.join(os.getcwd(), "media_mailing_cpf", "viabilidades_credito_enriquecido")
    cols_telefone = [f"Telefone_{i}" for i in range(1,21)]
    colunas_esperadas = ["cpf", "nome", "endereco", "numero", "complemento","cep", "bairro","cidade", "uf", "celular_1", "celular_2", "celular_3", "renda_presumida", "pasta"] + cols_telefone
    tipos_viabilidade = ["Primaria_", "Secundaria_"]
    telefones_encontrados = []
    for estado in ESTADOS_BR:
        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}{estado}.csv"
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
                salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} não possui as colunas esperadas. Esperadas: {colunas_esperadas}.Encontradas: {df.columns.to_list()}",status="Erro")            
                return False
            
            
            

            for col_tel in cols_telefone:
                df_repetidos = df[df[col_tel].isin(telefones_encontrados)]
                if len(df_repetidos.index) > 1:
                    salva_status(nova_execucao, titulo=f"Erro encontrar telefones para enriquecimento. Arquivo {file} possui telefones repetidos com outro arquivo",status="Erro")            

                    return False
                df_tels = df[df[col_tel].astype(str).str.len() > 3]
                teles = df_tels[col_tel].unique().tolist()

                telefones_encontrados += teles
            
            salva_status(nova_execucao, titulo=f"Total de telefones encontrados até o momento: {len(telefones_encontrados)}",status="Em Andamento")            

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
    if sistema == "giga_mais":
        colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3"] + cols_telefone
    else:
        colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3", "credito"] + cols_telefone
    cnpjs_encontrados = []
    telefones_encontrados = []

    if sistema == "giga_mais":
        tipos_viabilidade = ["Primaria_",]
    else:
        tipos_viabilidade = ["Primaria_", "Secundaria_"]
    for estado in ESTADOS_BR:
        for tipo in tipos_viabilidade:
            file = f"Viabilidade_{tipo}{estado}.csv"
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
                df_tels = df[df[col_tel].astype(str).str.len() > 3]
                teles = df_tels[col_tel].unique().tolist()

                telefones_encontrados += teles
            
            salva_status(nova_execucao, titulo=f"Total de telefones encontrados até o momento: {len(telefones_encontrados)}",status="Em Andamento")            

            if sistema != "giga_mais":
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