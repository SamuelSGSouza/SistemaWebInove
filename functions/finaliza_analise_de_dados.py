import os, pandas as pd
from .contantes import *
from data.models import salva_dado

def conta_dados(sistema_original):
    sistemas = {
        "janeiro_2026": "media_janeiro_2026",
        "oi": "media",
        "giga_mais": "media_giga_mais",
        "mailing_cpfs": "media_mailing_cpf"
    }
    sistema = sistemas[sistema_original]
    total_empresas_receita = 0
    total_empresas_mei = 0
    total_empresas_nmei = 0
    for file in os.listdir(f"media/arquivos_receita_federal"):
        filepath = os.path.join(f"media/arquivos_receita_federal", file)
        estado = filepath.split(".")[0].split("_")[-1]
        tipo_viabilidade = filepath.split(".")[0].split("_")[-2]
        
        df_receita = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)
        salva_dado(f"Total de Empresas ATIVAS no estado {estado}", len(df_receita.index), sistema=sistema_original)
        total_empresas_receita += len(df_receita.index)

        df_unificado_meis = df_receita[df_receita["MEINAOMEI"] == "S"]
        salva_dado(f"Total de Empresas MEI ATIVAS no estado {estado}", len(df_unificado_meis.index), sistema=sistema_original)
        total_empresas_mei += len(df_unificado_meis.index)

        df_unificado_Nmeis = df_receita[df_receita["MEINAOMEI"] != "S"]
        salva_dado(f"Total de Empresas NÃO-MEI ATIVAS no estado {estado}", len(df_unificado_Nmeis.index), sistema=sistema_original)
        total_empresas_nmei += len(df_unificado_Nmeis.index)

    salva_dado(f"Total de Empresas ATIVAS somantos TODOS os estados", total_empresas_receita, sistema=sistema_original)
    salva_dado(f"Total de Empresas MEI ATIVAS somantos TODOS os estados", total_empresas_mei, sistema=sistema_original)
    salva_dado(f"Total de Empresas NÃO-MEI ATIVAS somantos TODOS os estados", total_empresas_nmei, sistema=sistema_original)

    # Create your views here.
    for file in os.listdir(f"{sistema}/viabilidades"):
        filepath = os.path.join(f"{sistema}/viabilidades", file)
        estado = filepath.split(".")[0].split("_")[-1]
        tipo_viabilidade = filepath.split(".")[0].split("_")[-2]

        quantidade = len(pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL).index)
        salva_dado(f"Quantidade de Empresas com Viabilidade {tipo_viabilidade} no Estado {estado}", quantidade, sistema=sistema_original)



    for file in os.listdir(f"{sistema}/viabilidades_credito_enriquecido"):
        filepath = os.path.join(f"{sistema}/viabilidades_credito_enriquecido", file)
        estado = filepath.split(".")[0].split("_")[-1]
        tipo_viabilidade = filepath.split(".")[0].split("_")[-2]

        df_viabilidade = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)

        df_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] == "S"]
        df_N_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] != "S"]
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - MEI", 
            len(df_meis[df_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist()), sistema=sistema_original
        )
        salva_dado(
                f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - NAO MEI", 
                len(df_N_meis[df_N_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist()), sistema=sistema_original
            )

        salva_dado(
                f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - MEI", 
                len(df_meis[df_meis["credito"] == "Negado"]["cnpj"].unique().tolist()), sistema=sistema_original
            )
        salva_dado(
            f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - NAO MEI", 
            len(df_N_meis[df_N_meis["credito"] == "Negado"]["cnpj"].unique().tolist()), sistema=sistema_original
        )
        salva_dado(
                f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e sem infos de crédito no estado {estado} - NAO MEI", 
                len(df_N_meis[df_N_meis["credito"] == "Sem Infos"]["cnpj"].unique().tolist()), sistema=sistema_original
            )

        salva_dado(
                f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e sem infos de crédito no estado {estado} - MEI", 
                len(df_meis[df_meis["credito"] == "Sem Infos"]["cnpj"].unique().tolist()), sistema=sistema_original
            )
        