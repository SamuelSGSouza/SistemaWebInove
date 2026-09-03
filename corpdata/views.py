from django.shortcuts import render
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from .models import *
import re


#####################################################
# DJANGO MODELS                                     #   
#####################################################
from django.contrib.postgres.aggregates import StringAgg
from django.db.models import Q, F, Value, Exists, OuterRef, TextField
from django.db.models.functions import Coalesce
from django.db import connection

LINHAS_POR_ARQUIVO = 500_000
UFs_VALIDAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
    "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
    "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}

import json
 
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
 
from .models import *
 
 
# Teto de segurança: evita uma query com uma lista gigante no IN.
MAX_CNPJS_POR_LOTE = 500
 
 
#####################################################
# HELPERS COMPARTILHADOS                            #
#####################################################
 
def apenas_digitos(valor) -> str:
    return "".join(c for c in str(valor or "") if c.isdigit())
 
 
def formatar_cnpj(cnpj: str) -> str:
    """Recebe 14 dígitos e devolve no formato armazenado no banco."""
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
 
 
def queryset_empresa_detalhe():
    """Queryset com os joins necessários para serializar uma empresa."""
    return (
        Empresa.objects
        .select_related(
            "natureza_juridica",
            "cnae_fiscal",
            "municipio",
        )
        .prefetch_related("cnaes_secundarios")
    )
 
 
def serializa_empresa(empresa) -> dict:
    """Formato único de saída, usado pela pesquisa individual e pela em lote."""
    return {
        "cnpj": empresa.cnpj,
        "data_inicio_atividades": empresa.data_inicio_atividades,
 
        "natureza_juridica": {
            "codigo": empresa.natureza_juridica.codigo,
            "descricao": empresa.natureza_juridica.descricao,
        } if empresa.natureza_juridica else None,
 
        "cnae_fiscal": {
            "codigo": empresa.cnae_fiscal.codigo,
            "descricao": empresa.cnae_fiscal.descricao,
        } if empresa.cnae_fiscal else None,
 
        "cnaes_secundarios": [
            {
                "codigo": cnae.codigo,
                "descricao": cnae.descricao,
            }
            for cnae in empresa.cnaes_secundarios.all()
        ],
 
        "razao_social": empresa.razao_social,
        "nome_fantasia": empresa.nome_fantasia,
        "matriz_filial": empresa.matriz_filial,
        "decisor": empresa.decisor,
        "situacao_cadastral": empresa.situacao_cadastral,
        "correio_eletronico": empresa.correio_eletronico,
 
        "endereco": {
            "logradouro": empresa.logradouro,
            "numero": empresa.numero,
            "complemento": empresa.complemento,
            "bairro": empresa.bairro,
            "cep": empresa.cep,
            "municipio": {
                "codigo": empresa.municipio.codigo,
                "nome": empresa.municipio.nome,
                "uf": empresa.municipio.uf,
            } if empresa.municipio else None,
        },
 
        "eh_mei": empresa.eh_mei,
 
        "telefones": [
            telefone
            for telefone in [
                empresa.telefone_receita_1,
                empresa.telefone_receita_2,
                empresa.telefone_receita_3,
            ]
            if telefone
        ],
 
        "viabilidade": empresa.viabilidade,
        "credito": empresa.credito,
    }
 
 
#####################################################
# PESQUISA INDIVIDUAL (refatorada)                  #
#####################################################
 
def pesquisa_empresa(request):
    cnpj = apenas_digitos(request.GET.get("cnpj", ""))
 
    if len(cnpj) != 14:
        return JsonResponse({
            "sucesso": False,
            "mensagem": "O CNPJ deve possuir 14 dígitos."
        }, status=400)
 
    empresa = get_object_or_404(
        queryset_empresa_detalhe(),
        cnpj=formatar_cnpj(cnpj),
    )
 
    return JsonResponse({
        "sucesso": True,
        "mensagem": "",
        "data": serializa_empresa(empresa),
    })
 
 
#####################################################
# PESQUISA EM LOTE                                  #
#####################################################
 
def _extrai_lista_cnpjs(request):
    """
    Aceita:
      POST  body JSON: {"cnpjs": ["11.222.333/0001-81", "11222333000181"]}
      GET   querystring: ?cnpjs=11222333000181,11222333000182
 
    Retorna (erro_response, lista_bruta).
    """
    if request.method == "POST":
        try:
            body = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse({
                "sucesso": False,
                "mensagem": "Corpo da requisição não é um JSON válido.",
            }, status=400), None
 
        if isinstance(body, list):
            bruto = body
        elif isinstance(body, dict):
            bruto = body.get("cnpjs")
        else:
            bruto = None
 
        if not isinstance(bruto, list):
            return JsonResponse({
                "sucesso": False,
                "mensagem": 'Envie um JSON no formato {"cnpjs": ["...", "..."]}.',
            }, status=400), None
 
        return None, bruto
 
    bruto = request.GET.get("cnpjs", "")
    return None, [parte for parte in bruto.split(",") if parte.strip()]
 
 
@csrf_exempt
@require_http_methods(["GET", "POST"])
def pesquisa_empresas_em_lote(request):
    erro, cnpjs_brutos = _extrai_lista_cnpjs(request)
 
    if erro is not None:
        return erro
 
    if not cnpjs_brutos:
        return JsonResponse({
            "sucesso": False,
            "mensagem": "Nenhum CNPJ informado.",
        }, status=400)
 
    if len(cnpjs_brutos) > MAX_CNPJS_POR_LOTE:
        return JsonResponse({
            "sucesso": False,
            "mensagem": (
                f"Foram enviados {len(cnpjs_brutos)} CNPJs. "
                f"O limite por requisição é de {MAX_CNPJS_POR_LOTE}."
            ),
        }, status=400)
 
    # ---------------------------------------------------------
    # Normalização: valida, remove duplicados e preserva a ordem
    # ---------------------------------------------------------
 
    invalidos = []
    formatados = []       # ordem de entrada, sem repetições
    vistos = set()
 
    for bruto in cnpjs_brutos:
        digitos = apenas_digitos(bruto)
 
        if len(digitos) != 14:
            invalidos.append(str(bruto).strip())
            continue
 
        formatado = formatar_cnpj(digitos)
 
        if formatado in vistos:
            continue
 
        vistos.add(formatado)
        formatados.append(formatado)
 
    if not formatados:
        return JsonResponse({
            "sucesso": False,
            "mensagem": "Nenhum CNPJ válido foi informado.",
            "invalidos": invalidos,
        }, status=400)
 
    # ---------------------------------------------------------
    # Consulta única no banco
    # ---------------------------------------------------------
 
    empresas = {
        empresa.cnpj: empresa
        for empresa in queryset_empresa_detalhe().filter(cnpj__in=formatados)
    }
 
    data = []
    nao_encontrados = []
 
    for formatado in formatados:
        empresa = empresas.get(formatado)
 
        if empresa is None:
            nao_encontrados.append(formatado)
            continue
 
        data.append(serializa_empresa(empresa))
 
    return JsonResponse({
        "sucesso": True,
        "mensagem": "",
        "total_solicitados": len(formatados),
        "total_encontrados": len(data),
        "data": data,
        "nao_encontrados": nao_encontrados,
        "invalidos": invalidos,
    })


def valida_filtros(filtros: dict):
    """
    Valida e normaliza os filtros utilizados pela monta_query().

    Retorna:
        - (None, filtros_validados) se estiver tudo certo
        - (JsonResponse, None) se houver erro
    """

    if not isinstance(filtros, dict):
        return JsonResponse(
            {
                "erro": "Os filtros devem ser enviados como um objeto JSON."
            },
            status=400
        ), None

    filtros_validos = {}

    # ---------------------------------------------------------
    # Campos permitidos
    # ---------------------------------------------------------

    campos_permitidos = {
        "cnpj",
        "razao_social",
        "nome_fantasia",
        "municipio",
        "uf",
        "situacao_cadastral",
        "cnae_fiscal",
        'eh_mei',
        "pagina"
    }

    campos_invalidos = set(filtros) - campos_permitidos

    if campos_invalidos:
        return JsonResponse(
            {
                "erro": "Foram enviados filtros desconhecidos.",
                "campos_invalidos": sorted(campos_invalidos),
                "campos_permitidos": sorted(campos_permitidos),
            },
            status=400
        ), None

    # ---------------------------------------------------------
    # Normalização básica
    # ---------------------------------------------------------

    for campo in campos_permitidos:
        valor = filtros.get(campo, "")

        if valor is None:
            valor = ""

        if not isinstance(valor, str):
            return JsonResponse(
                {
                    "erro": f'O filtro "{campo}" deve ser uma string.'
                },
                status=400
            ), None

        filtros_validos[campo] = valor.strip()

    # ---------------------------------------------------------
    # CNPJ
    # ---------------------------------------------------------

    cnpj = filtros_validos["cnpj"]

    if cnpj:
        cnpj_numeros = "".join(c for c in cnpj if c.isdigit())

        if len(cnpj_numeros) != 14:
            return JsonResponse(
                {
                    "erro": "CNPJ inválido.",
                    "motivo": "O CNPJ deve possuir 14 dígitos."
                },
                status=400
            ), None

        filtros_validos["cnpj"] = cnpj

    # ---------------------------------------------------------
    # UF
    # ---------------------------------------------------------

    uf = filtros_validos["uf"].upper()

    if uf:
        if uf not in UFs_VALIDAS:
            return JsonResponse(
                {
                    "erro": "UF inválida.",
                    "uf_recebida": uf,
                    "ufs_validas": sorted(UFs_VALIDAS),
                },
                status=400
            ), None

        filtros_validos["uf"] = uf

    # ---------------------------------------------------------
    # Situação cadastral
    # ---------------------------------------------------------

    situacoes_validas = {
        "01",  # NULA
        "02",  # ATIVA
        "03",  # SUSPENSA
        "04",  # INAPTA
        "08",  # BAIXADA
    }

    situacao = filtros_validos["situacao_cadastral"]

    if situacao:
        if situacao not in situacoes_validas:
            return JsonResponse(
                {
                    "erro": "Situação cadastral inválida.",
                    "situacao_recebida": situacao,
                    "situacoes_validas": sorted(situacoes_validas),
                },
                status=400
            ), None

    # ---------------------------------------------------------
    # CNAE
    # ---------------------------------------------------------

    cnae = filtros_validos["cnae_fiscal"]

    if cnae:
        cnae_numeros = "".join(c for c in cnae if c.isdigit())

        if len(cnae_numeros) != 7:
            return JsonResponse(
                {
                    "erro": "CNAE inválido.",
                    "motivo": "O código CNAE deve possuir 7 dígitos."
                },
                status=400
            ), None

        filtros_validos["cnae_fiscal"] = cnae_numeros

    # ---------------------------------------------------------
    # Validação de pelo menos um filtro
    # ---------------------------------------------------------

    if not any(filtros_validos.values()):
        return JsonResponse(
            {
                "erro": "Nenhum filtro informado.",
                "motivo": "Informe pelo menos um filtro para realizar a pesquisa."
            },
            status=400
        ), None

    # ---------------------------------------------------------
    # Validação da pesquisa por nome
    # ---------------------------------------------------------

    razao_social = filtros_validos["razao_social"]
    if razao_social and len(str(razao_social)) < 5:
        return JsonResponse({
                "erro": "Razão Social Inválida.",
                "motivo": "Quando razão social é informada, deve conter ao mínimo 6 caracteres."
            },
            status=400
        ), None
    
    # ---------------------------------------------------------
    # Validação da paginação
    # ---------------------------------------------------------

    pagina = filtros_validos["pagina"]
    if pagina and not isinstance(pagina, int):
        return JsonResponse({
                "erro": "Página solicitada Inválida.",
                "motivo": f"A página passada deve ser um número inteiro. Página solicitada: {pagina}"
            },
            status=400
        ), None

    # ---------------------------------------------------------
    # Geral dos filtros
    # ---------------------------------------------------------
    if not uf and not cnpj and not cnae:
        return JsonResponse({
                "erro": "Filtros Geral Inválido.",
                "motivo": f"Ao menos um dos campos entre 'cnpj', 'uf' ou 'cnae' deve ser filtrado"
            },
            status=400
        ), None

    
    # ---------------------------------------------------------
    # Filtro de porte da empresa
    # ---------------------------------------------------------
    
    eh_mei = filtros_validos["eh_mei"]
    print(filtros_validos)
    if eh_mei is not None and len(str(eh_mei))>3:

        if not eh_mei in ["false", "true"]:
            return JsonResponse({
                    "erro": "Seleção de Mei / Não Mei inválida.",
                    "motivo": f"O campo de Mei / Não Mei deve ser um booleano."
                },
                status=400
            ), None


    return None, filtros_validos

def monta_query(filtros: dict, pagina=None, limit=500_000):
    qs = Empresa.objects.all()

    cnpj = filtros.get("cnpj", "")
    razao_social = filtros.get("razao_social", "")
    nome_fantasia = filtros.get("nome_fantasia", "")
    municipio = filtros.get("municipio", "")
    uf = filtros.get("uf", "")
    situacao = filtros.get("situacao_cadastral", "")
    cnae = filtros.get("cnae_fiscal", "")
    eh_mei = filtros.get("eh_mei", None)

    if cnpj:
        qs = qs.filter(cnpj=cnpj)

    if razao_social:
        qs = qs.filter(razao_social__icontains=razao_social)

    if nome_fantasia:
        qs = qs.filter(nome_fantasia__icontains=nome_fantasia)

    if municipio:
        qs = qs.filter(municipio__nome__icontains=municipio)

    if uf:
        qs = qs.filter(municipio__uf=uf)

    if situacao:
        qs = qs.filter(situacao_cadastral=situacao)

    if cnae:
        secundarios = Cnae.objects.filter(
            empresas=OuterRef("pk"),
            codigo=cnae,
        )

        qs = qs.filter(
            Q(cnae_fiscal__codigo=cnae) | Exists(secundarios)
        )

    if eh_mei is not None:
        qs = qs.filter(eh_mei=eh_mei)

    qs = (
        qs
        .annotate(
            cnaes_secundarios_list=Coalesce(
                SQLiteGroupConcat(
                    "cnaes_secundarios__codigo",
                ),
                Value("", output_field=TextField()),
                output_field=TextField(),
            )
        )
        .order_by("id")
    )

    if pagina is not None:
        qs = qs.filter(id__gt=int(pagina))

    return qs[:limit]


def dados_empresas_em_lote(request):
    limit = 500_000

    filtros = {
        "cnpj": request.GET.get("cnpj", ""),
        "razao_social": request.GET.get("razao_social", ""),
        "nome_fantasia": request.GET.get("nome_fantasia", ""),
        "municipio": request.GET.get("municipio", ""),
        "uf": request.GET.get("uf", ""),
        "situacao_cadastral": request.GET.get("situacao_cadastral", ""),
        "cnae_fiscal": request.GET.get("cnae_fiscal", ""),
        "eh_mei": request.GET.get("eh_mei", None),
    }


    erro, filtros = valida_filtros(filtros)

    if erro is not None:
        return erro

    pagina = request.GET.get("after_id")
    qs = monta_query(filtros, pagina, limit)

    resultados = list(
        qs.values(
            "id",
            "cnpj",
            "municipio__nome",
            "municipio__uf",
            "cnae_fiscal__codigo",
            "natureza_juridica__codigo",
            "natureza_juridica__descricao",
            "cnaes_secundarios_list",
            "data_inicio_atividades",
            "razao_social",
            "nome_fantasia",
            "matriz_filial",
            "decisor",
            "situacao_cadastral",
            "correio_eletronico",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "eh_mei",
            "telefone_receita_1",
            "telefone_receita_2",
            "telefone_receita_3",
        )
    )

    next_cursor = (
        resultados[-1]["id"]
        if len(resultados) == limit
        else None
    )

    return JsonResponse({
        "resultados": resultados,
        "next_cursor": next_cursor,
    })