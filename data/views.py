from django.shortcuts import render
from django.shortcuts import render, redirect
from django.urls import reverse
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
# from .forms import LoginForm
from .models import *
# from universal_data.models import ResultadoExtracao
# from functions.gera_mailing import *
from functions.utils import *
# from universal_data.models import *
from django.views.generic import TemplateView
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
import os, datetime,threading, pandas as pd
from django.conf import settings
from typing import Union
from django.http import StreamingHttpResponse
from multiprocessing import Process   
from django.contrib.auth.mixins import LoginRequiredMixin
import traceback, shutil
from django.http import FileResponse, Http404
from django.conf import settings
import re

from django.db.models import Count
from django.core.paginator import Paginator
import json
from functions.contantes import *
from functions.gerador import inicia_gerador

# Create your views here.
class Dashboard(LoginRequiredMixin,TemplateView):
    template_name = "dashboard.html"

# status = Status_Execucoe_DB.objects.create(
#     sistema="oi"
# )
# Fase_Execucao_DB.objects.create(
#     status_execucao=status,
#     titulo="Verificação de Viabilidades"
# )
# Fase_Execucao_DB.objects.create(
#     status_execucao=status,
#     titulo="Análise de Crédito"
# )
# Fase_Execucao_DB.objects.create(
#     status_execucao=status,
#     titulo="Preenchimento de telefones"
# )

class Status_Execucao(LoginRequiredMixin,TemplateView):
    template_name = "status_execucao.html"
    def get_context_data(self, **kwargs):
        context =  super().get_context_data(**kwargs)

        sistema = self.request.GET.get("sistema", "oi")
        titulos = {
            'oi': "Mailing Original",
            'geral': "Mailing Original",
            'giga_mais': "Mailing Giga +",
            'janeiro_2026': "Mailing Janeiro 2026"
        }
        context["sistema"] = sistema
        context['titulo'] = titulos[sistema]
        self.request.session["sistema"] = sistema

        context["acompanhar_gerador_activate"] = "active"

        possiveis_status = Status_Execucoe_DB.objects.filter(sistema=sistema)
        if possiveis_status.exists():
            status = possiveis_status[0]
            context["data_inicializacao"] = status.momento_inicializacao
            context["data_finalizacao"] = status.momento_finalizacao

            context["fases"] = Fase_Execucao_DB.objects.filter(status_execucao=status).order_by("id")
            context["fase_atual"] = context["fases"][len(context["fases"])-1].titulo
        return context



def filtra_mailing_view(request):    


    context = {
        'nome_dados': 'Empresas',
        'estados': ESTADOS_BR,
        'resultados': None,
        'qtd_resultados': 0,
        'colunas': [],
        'cnaes': get_cnaes()
    }
    nome_padrao_arquivo = ""
    PASTAS_RAIZ = {
            "oi": os.path.join(os.getcwd(), "media"),
            "geral": os.path.join(os.getcwd(), "media"),
            "giga_mais": os.path.join(os.getcwd(), "media_giga_mais"),
            "janeiro_2026": os.path.join(os.getcwd(), "media_janeiro_2026")
        }
    pasta_raiz = PASTAS_RAIZ[request.session["sistema"]]

    filepath_csv = os.path.join(os.getcwd(), f"{pasta_raiz}/{request.user.username}_arquivos_mailing_filtrados")
    os.makedirs(filepath_csv, exist_ok=True)

    
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))
    if request.method == 'POST':
        try:
            # Processar filtros
            filtros = {}

            formato_saida = request.POST.get("formato_saida", "Padrão")
            

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            print("ESTADOS: ", estados)
            if estados != []:
                estados = estados
            else:
                estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
            if estados:
                filtros['uf'] = estados
                

            # # CNAE (múltiplos valores)
            cnaes = request.POST.getlist('cnae_list', [])
            if cnaes:
                filtros['cnae_fiscal'] = cnaes

            # # Município (múltiplos valores)
            # municipios_raw = request.POST.get('municipio', '')
            # municipios = [m.strip() for m in municipios_raw.split(',') if m.strip()]
            # if municipios:
            #     filtros['municipio'] = municipios

            # # Bairro (múltiplos valores)
            # bairros_raw = request.POST.get('bairro', '')
            # bairros = [b.strip() for b in bairros_raw.split(',') if b.strip()]
            # if bairros:
            #     filtros['bairro'] = bairros

            # termos_chave = request.POST.get("termos_chave", "")
            # if termos_chave:
            #     filtros["termos_chave"] = termos_chave

            tipo_empresa = request.POST.get("tipoEmpresa", "")
            if tipo_empresa and tipo_empresa != "Ambos":
                filtros["MEINAOMEI"] = tipo_empresa


            conjunto_telefones = request.POST.get("conjuntoTelefone", "")
            tipos_telefone = request.POST.get("tipoTelefone", "")
            tipoMailing = request.POST.get("tipoMailing", "")
            filtro_telefone_blacklist = request.POST.get("filtro_telefone_blacklist", "")

            checkbox_credito_preaprovado = request.POST.get("checkbox_credito_preaprovado", "")
            checkbox_pre_negado = request.POST.get("checkbox_pre_negado", "")
            checkbox_sem_info_credito = request.POST.get("checkbox_sem_info_credito", "")

            # Obter dados do CSV
            dfs = []
            pastas = {
            }
            if checkbox_credito_preaprovado:
                pastas[f"{pasta_raiz}/viabilidades_credito_enriquecido"] = "APROVADO"
                

            if checkbox_pre_negado:
                pastas[f"{pasta_raiz}/viabilidades_credito_pre_negado_enriquecido"] = "PRENEGADO"


            if checkbox_sem_info_credito:
                pastas[f"{pasta_raiz}/viabilidades_credito_nao_informado_enriquecido"] = "SEM INFO"

            if len(list(pastas.keys())) == 0:
                df = pd.DataFrame()
                df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                context['resultados'] = 0
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)

                return render(request, 'filtros_mailing.html', context)

            print("FILTROS: ", filtros)
            for pasta in list(pastas.keys()):
                df = get_dados_mailing(filtros, pasta_selecionada=pasta, formato_saida=formato_saida, conjunto_telefones=conjunto_telefones, tipos_telefone= tipos_telefone, tipoMailing=tipoMailing, filtro_telefone_blacklist=filtro_telefone_blacklist)
                df["CREDITO"] =  pastas[pasta]
                dfs.append(df)

            df = pd.concat(dfs)
            df.drop_duplicates(subset=["cnpj"], keep="first",inplace=True)
            

            meses = {
                "1": "Janeiro",
                "2": "Fevereiro",
                "3": "Março",
                "4": "Abril",
                "5": "Maio",
                "6": "Junho",
                "7": "Julho",
                "8": "Agosto",
                "9": "Setembro",
                "10": "Outubro",
                "11": "Novembro",
                "12": "Dezembro"
            }
            dia = datetime.datetime.now().day 
            dia = str(dia) if dia > 9 else "0"+ str(dia)
            data_atual = f'{dia}-{meses[str(datetime.datetime.now().month)]}'
            nome_padrao_arquivo += data_atual
            # Preparar dados para exibição
            if not df.empty:
                
                max_linhas = 200_000

                if len(df.index) > max_linhas:
                    # Divide em pedaços de 200k
                    f = 0
                    for i in range(0, len(df), max_linhas):
                        nome_arquivo = nome_padrao_arquivo + f"_parte_{f}" + ".csv"
                        df.iloc[i:i + max_linhas].to_csv(os.path.join(filepath_csv, nome_arquivo),sep=";", index=False)
                        f+=1
                else:
                    df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                zip_folder(filepath_csv, f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip")

                context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)

                response = FileResponse(open(f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip", 'rb'), as_attachment=True, filename='dados_filtrados.zip')
                return response
        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtra_mailing.html', context)

def inicia_gerador_view(request):
    # if request.method != 'POST':
    #     return JsonResponse({'status': 'error', 'sucessos': [], "erros":['Método inválido.',], "links": [], "relatorio": []})

    sistema = request.GET.get("sistema", "oi")
    processo = threading.Thread(target=inicia_gerador, args=(sistema,))
    processo.start()

    return JsonResponse({'status': 'success', 'sucessos': [], "erros":[], "links": [], "relatorio": []})


def filtro_geral_view(request):
    context = {
        'nome_dados': 'Empresas',
        'estados_municipios': DICT_ESTADOS_MUNICIPIOS,
        'cnaes': get_cnaes()
    }
    return render(request, 'filtro_geral.html', context)

    nome_padrao_arquivo = ""
    filepath_csv = os.path.join(os.getcwd(), "media/arquivos_receita_federal_filtrados")
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))
    if request.method == 'POST':
        try:
            # Processar filtros
            filtros = {}

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            if estados != [""]:
                estados = [e.strip() for e in estados if e.strip()][0].split(",")
            else:
                estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
                        ]
            if estados:
                filtros['uf'] = estados
                for estado in estados:
                    nome_padrao_arquivo += f"{estado}_"

            # CNAE (múltiplos valores)
            cnaes_raw = request.POST.get('cnae', '')
            cnaes = [c.strip() for c in cnaes_raw.split(',') if c.strip()]
            if cnaes:
                filtros['cnae_fiscal'] = cnaes

            # Município (múltiplos valores)
            municipios_raw = request.POST.get('municipio', '')
            municipios = [m.strip() for m in municipios_raw.split(',') if m.strip()]
            if municipios:
                filtros['municipio'] = municipios

            # Bairro (múltiplos valores)
            bairros_raw = request.POST.get('bairro', '')
            bairros = [b.strip() for b in bairros_raw.split(',') if b.strip()]
            if bairros:
                filtros['bairro'] = bairros

            termos_chave = request.POST.get("termos_chave", "")
            if termos_chave:
                filtros["termos_chave"] = termos_chave

            tipo_empresa = request.POST.get("tipoEmpresa", "")
            if tipo_empresa:
                filtros["MEINAOMEI"] = tipo_empresa

            # Obter dados do CSV
            df = get_dados_csv(filtros)
            # Converter colunas categóricas para strings
            for col in df.select_dtypes(include=['category']).columns:
                df[col] = df[col].astype(str)



            tipoTelefone = request.POST.get('tipoTelefone', '')
            if tipoTelefone == "apenas_movel":
                df = remove_fixos(df) 

            # Agora pode usar replace e fillna tranquilamente
            df["cnpj"] = df["cnpj"].apply(lambda x: re.sub(r'[^0-9]', '', x))
            df = padronizacao(df.replace(",0", "").replace(".0", "").fillna("")).reset_index(drop=True)

            meses = {
                "1": "Janeiro",
                "2": "Fevereiro",
                "3": "Março",
                "4": "Abril",
                "5": "Maio",
                "6": "Junho",
                "7": "Julho",
                "8": "Agosto",
                "9": "Setembro",
                "10": "Outubro",
                "11": "Novembro",
                "12": "Dezembro"
            }
            dia = datetime.datetime.now().day 
            dia = str(dia) if dia > 9 else "0"+ str(dia)
            data_atual = f'{dia}-{meses[str(datetime.datetime.now().month)]}'
            nome_padrao_arquivo += data_atual
            # Preparar dados para exibição
            
            if len(df.index) > 0:
                print("df vazio não será salvo")
                max_linhas = 200_000

                if len(df.index) > max_linhas:
                    # Divide em pedaços de 200k
                    f = 0
                    for i in range(0, len(df), max_linhas):
                        nome_arquivo = nome_padrao_arquivo + f"_parte_{f}" + ".csv"
                        df.iloc[i:i + max_linhas].to_csv(os.path.join(filepath_csv, nome_arquivo),sep=";", index=False)
                        f+=1
                else:
                    df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                zip_folder(filepath_csv, f"media/{request.user.username}_filtrados.zip")

                context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)
        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtros.html', context)