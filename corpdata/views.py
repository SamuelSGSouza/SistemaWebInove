from django.shortcuts import render
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from .models import Empresa

def pesquisa_empresa(request):

    def formatar_cnpj(cnpj):
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

    cnpj = request.GET.get("cnpj", "").replace(".", "").replace("/", "").replace("-", "")

    if len(cnpj) != 14 or not cnpj.isdigit():
        return JsonResponse({
            "sucesso": False,
            "mensagem": "O CNPJ deve possuir 14 dígitos."
        }, status=400)

    empresa = get_object_or_404(
        Empresa.objects.select_related(
            "natureza_juridica",
            "cnae_fiscal",
            "municipio",
        ),
        cnpj=formatar_cnpj(cnpj)
    )

    data = {
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

        "cpf": empresa.cpf,
        "mei_nome_mei": empresa.mei_nome_mei,

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
    }

    return JsonResponse({
        "sucesso": True,
        "mensagem": "",
        "data": data,
    })

def pesquisa_empresas(request):

    qs = Empresa.objects.select_related(
        "natureza_juridica",
        "cnae_fiscal",
        "municipio",
    ).prefetch_related(
        "cnaes_secundarios"
    )

    # -------------------------
    # Filtros
    # -------------------------

    cnpj = request.GET.get("cnpj", "").strip()
    razao_social = request.GET.get("razao_social", "").strip()
    nome_fantasia = request.GET.get("nome_fantasia", "").strip()
    municipio = request.GET.get("municipio", "").strip()
    uf = request.GET.get("uf", "").strip()
    situacao = request.GET.get("situacao_cadastral", "").strip()
    cnae = request.GET.get("cnae_fiscal", "").strip()

    if cnpj:
        cnpj = (
            cnpj
            .replace(".", "")
            .replace("/", "")
            .replace("-", "")
        )

        if not cnpj.isdigit() or len(cnpj) != 14:
            return JsonResponse({
                "sucesso": False,
                "mensagem": "O CNPJ deve possuir 14 dígitos."
            }, status=400)

        cnpj_formatado = (
            f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/"
            f"{cnpj[8:12]}-{cnpj[12:14]}"
        )

        qs = qs.filter(cnpj=cnpj_formatado)

    if razao_social:
        qs = qs.filter(
            razao_social__icontains=razao_social
        )

    if nome_fantasia:
        qs = qs.filter(
            nome_fantasia__icontains=nome_fantasia
        )

    if municipio:
        qs = qs.filter(
            municipio__nome__icontains=municipio
        )

    if uf:
        qs = qs.filter(
            municipio__uf=uf.upper()
        )

    if situacao:
        qs = qs.filter(
            situacao_cadastral=situacao
        )

    if cnae:
        qs = qs.filter(
            cnae_fiscal__codigo=cnae
        )

    # -------------------------
    # Paginação
    # -------------------------

    try:
        limit_int = int(request.GET.get("limit", 0))
        limit = min(limit_int, 100)
        if limit_int < 1:
            limit = max(1, 10000000000)
        
        offset = max(int(request.GET.get("offset", 0)), 0)
    except ValueError:
        return JsonResponse({
            "sucesso": False,
            "mensagem": "limit e offset devem ser números inteiros."
        }, status=400)

    total = qs.count()

    empresas = qs[offset:offset + limit]
    print("Total de empresas: ",len(empresas))
    # -------------------------
    # Serialização
    # -------------------------

    data = []

    for empresa in empresas:

        data.append({
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

            "cpf": empresa.cpf,
            "mei_nome_mei": empresa.mei_nome_mei,

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
        })

    return JsonResponse({
        "sucesso": True,
        "mensagem": "",
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data,
    })