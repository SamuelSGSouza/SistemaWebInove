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