import os, re
from .contantes import *
from data.models import *
from corpdata.models import Telefones
import pandas as pd
from .utils import clean_phone_number
from pathlib import Path
from datetime import datetime, timedelta
import traceback

def fase_4_enriquecer(sistema, nova_execucao):
    salva_status(nova_execucao, titulo="Iniciando coleta de enriquecimento de telefones", status="Em Andamento")
 
    try:
        raiz = os.path.join(os.getcwd(), PASTAS_RAIZ[sistema])
 
        if sistema == "mailing_cpfs":
            campo_referencia_documento = "cpf"
            quantidade_caracteres = 11
            cols_telefone_originais = ["celular_1", "celular_2", "celular_3"]
            dtypes_especifico = str
            viabilidades_path = os.path.join(raiz, "viabilidades")
        else:
            campo_referencia_documento = "cnpj"
            quantidade_caracteres = 14
            cols_telefone_originais = ["TEL1", "TEL2", "TEL3"]
            dtypes_especifico = DTYPES_RECEITA_FEDERAL
            viabilidades_path = os.path.join(raiz, "viabilidades" if sistema == "giga_mais" else "viabilidades_credito")
 
        enriquecido_path = os.path.join(raiz, "viabilidades_credito_enriquecido")
        os.makedirs(enriquecido_path, exist_ok=True)
        for file in os.listdir(enriquecido_path):
            os.remove(os.path.join(enriquecido_path, file))
 
        # 1) Carrega a base de telefones agrupada por documento: {documento: [telefone, ...]}
        telefones_por_documento = {}
        for documento, telefone in Telefones.objects.values_list("documento", "telefone").iterator(chunk_size=100_000):
            telefones_por_documento.setdefault(documento, []).append(telefone)
 
        if len(telefones_por_documento) < 1_000_000:
            salva_status(nova_execucao, titulo="Erro ao encontrar telefones para enriquecimento. A base de telefones não possui a quantidade de documentos esperada", status="Erro")
            return
 
        cols_telefone = [f"Telefone_{i}" for i in range(1, 21)]
        todos_telefones = set()  # garante que cada telefone apareça uma única vez entre todos os arquivos
 
        # 2) Enriquece cada arquivo de viabilidade --------------------------------------
        for file in os.listdir(viabilidades_path):
            tipo_viabilidade, estado = os.path.splitext(file)[0].split("_")[-2:]
            salva_status(nova_execucao, titulo=f"Iniciando análise no estado {estado}", status="Em Andamento")
            salva_status(nova_execucao, titulo=f"Encontrando telefones adicionais para os {campo_referencia_documento} do tipo {tipo_viabilidade} no estado {estado}", status="Em Andamento")
 
            filepath = os.path.join(viabilidades_path, file)
            df = pd.read_csv(filepath, sep=";", dtype=dtypes_especifico)
 
            if campo_referencia_documento not in df.columns:
                salva_status(nova_execucao, titulo=f"Erro! Campo de referencia >>{campo_referencia_documento}<< não existe no arquivo >>{filepath}<<", status="Erro")
                return
 
            df[campo_referencia_documento] = (
                df[campo_referencia_documento]
                .astype(str)
                .str.replace(r"\D", "", regex=True)
                .str.zfill(quantidade_caracteres)
            )
 
            # Telefones que já vieram no arquivo (têm prioridade sobre os da base)
            linhas = []
            for tels in df[cols_telefone_originais].itertuples(index=False):
                novos = [t for t in dict.fromkeys(map(clean_phone_number, tels)) if t and t not in todos_telefones]
                todos_telefones.update(novos)
                linhas.append(novos + [""] * (len(cols_telefone_originais) - len(novos)))
            df[cols_telefone_originais] = pd.DataFrame(linhas, index=df.index, columns=cols_telefone_originais)
 
            # Telefones adicionais vindos da base de telefones (até 20 por documento)
            linhas = []
            for documento in df[campo_referencia_documento]:
                tels = telefones_por_documento.get(documento, [])
                novos = [t for t in dict.fromkeys(map(clean_phone_number, tels)) if t and t not in todos_telefones][:20]
                todos_telefones.update(novos)
                linhas.append(novos + [""] * (20 - len(novos)))
            df[cols_telefone] = pd.DataFrame(linhas, index=df.index, columns=cols_telefone)
 
            df = df.drop(columns=["CHAVE_ESPECIFICA", "CHAVE_GERAL"])
 
            if campo_referencia_documento == "cnpj" and df["cnpj"].duplicated().any():
                salva_status(nova_execucao, titulo=f"Erro ao encontrar telefones para enriquecimento. Viabilidade do tipo {tipo_viabilidade} no estado {estado} ficou com cnpjs repetidos", status="Erro")
                return
 
            df.to_csv(os.path.join(enriquecido_path, file), sep=";", index=False)
 
        # 3) Valida o resultado ----------------------------------------------------------
        verificador = verificador_fase_4_cpfs if campo_referencia_documento == "cpf" else verificador_fase_4
 
        if verificador(sistema, nova_execucao):
            salva_status(nova_execucao, "Enriquecimento de telefones concluído.", status="Concluido")
            return True
 
    except Exception:
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