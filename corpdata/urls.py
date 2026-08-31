from django.urls import path
from .views import *

urlpatterns = [
    path('pesquisa_empresa', pesquisa_empresa, name="pesquisa_empresa"),

    path('dados_empresas_em_lote', dados_empresas_em_lote, name="dados_empresas_em_lote")
]