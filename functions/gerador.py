from functions.baixar_receita_federal import fase_1_gerador
from functions.concatena_receita_e_dfv import fase_2_concatenador
from functions.define_credito import fase_3_define_credito
from functions.encontra_telefones_adicionais import fase_4_enriquecer
from data.models import Status_Execucoe_DB
from functions.finaliza_analise_de_dados import conta_dados
from corpdata.models import Empresa, ResumoDados, ResumoDadosUF
from django.db.models import Count, Q
from django.db import transaction



def _agregacoes():
    """
    Monta o dicionário de Count(...) que é usado tanto no total geral
    quanto no agrupamento por UF (os dois models têm os mesmos campos).
    """
    mei = Q(eh_mei=True)
    nmei = ~Q(eh_mei=True)  # False ou nulo
 
    primaria = Q(viabilidade=Empresa.VIABILIDADE_PRIMARIA)
    secundaria = Q(viabilidade=Empresa.VIABILIDADE_SECUNDARIA)
    sem_viab = ~primaria & ~secundaria  # nulo, "" ou qualquer outro valor
 
    aprovado = Q(credito=Empresa.CREDITO_APROVADO)
    negado = Q(credito=Empresa.CREDITO_NEGADO)
    sem_credito = ~aprovado & ~negado
 
    return {
        "total_empresas": Count("id"),
        "total_empresas_mei": Count("id", filter=mei),
        "total_empresas_nmei": Count("id", filter=nmei),
 
        "total_empresas_viabilidade_primaria": Count("id", filter=primaria),
        "total_empresas_viabilidade_secundaria": Count("id", filter=secundaria),
        "total_empresas_viabilidade_nao_informada": Count("id", filter=sem_viab),
 
        "total_empresas_mei_viabilidade_primaria": Count("id", filter=mei & primaria),
        "total_empresas_mei_viabilidade_secundaria": Count("id", filter=mei & secundaria),
        "total_empresas_mei_viabilidade_nao_informada": Count("id", filter=mei & sem_viab),
 
        "total_empresas_nmei_viabilidade_primaria": Count("id", filter=nmei & primaria),
        "total_empresas_nmei_viabilidade_secundaria": Count("id", filter=nmei & secundaria),
        "total_empresas_nmei_viabilidade_nao_informada": Count("id", filter=nmei & sem_viab),
 
        "total_empresas_credito_aprovado": Count("id", filter=aprovado),
        "total_empresas_credito_negado": Count("id", filter=negado),
        "total_empresas_credito_sem_info": Count("id", filter=sem_credito),
 
        "total_empresas_credito_aprovado_mei": Count("id", filter=mei & aprovado),
        "total_empresas_credito_negado_mei": Count("id", filter=mei & negado),
        "total_empresas_credito_sem_info_mei": Count("id", filter=mei & sem_credito),
 
        "total_empresas_credito_aprovado_nmei": Count("id", filter=nmei & aprovado),
        "total_empresas_credito_negado_nmei": Count("id", filter=nmei & negado),
        "total_empresas_credito_sem_info_nmei": Count("id", filter=nmei & sem_credito),
    }
 
@transaction.atomic
def monta_resumo():
    """
    Cria um ResumoDados com os totais gerais e um ResumoDadosUF por UF.
    Faz apenas 2 queries de leitura no banco.
    """
    agregacoes = _agregacoes()
 
    # Totais gerais
    resumo = ResumoDados.objects.create(**Empresa.objects.aggregate(**agregacoes))
 
    # Totais por UF (empresas sem município ficam de fora)
    linhas = (
        Empresa.objects
        .filter(municipio__isnull=False)
        .order_by()
        .values("municipio__uf")
        .annotate(**agregacoes)
    )
 
    registros = []
    for linha in linhas:
        uf = linha.pop("municipio__uf")
        registros.append(ResumoDadosUF(resumo=resumo, uf=uf, **linha))
 
    ResumoDadosUF.objects.bulk_create(registros)
    print(f"Resumo de dados criado com {len(registros)} registros de UF.")
    return resumo


def inicia_gerador(sistema="oi"):
    fase_1_ok = fase_1_gerador()

    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)

    
    # if fase_2_ok:
    #     fase_3_ok = fase_3_define_credito(sistema=sistema, nova_execucao=nova_execucao)
    #     if fase_3_ok:
    #         fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)
    #         conta_dados(sistema)


    # inicia_gerador_mailing_2026()
    # conta_dados("janeiro_2026")


    # inicia_gerador_arquivos_cpf()
    # conta_dados("mailing_cpfs")

    monta_resumo()

def inicia_gerador_mailing_2026(sistema="janeiro_2026"):
    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
    if fase_2_ok:
        fase_3_ok = fase_3_define_credito(sistema=sistema, nova_execucao=nova_execucao)
        if fase_3_ok:
            fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)

def inicia_gerador_arquivos_cpf(sistema="mailing_cpfs"):
    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
    if fase_2_ok:
    
        fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)