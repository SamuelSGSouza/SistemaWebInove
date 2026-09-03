from django.urls import path
from .views import *

urlpatterns = [
    path('pesquisa_empresa', pesquisa_empresa, name="pesquisa_empresa"),

    path('pesquisa_empresas_em_lote', pesquisa_empresas_em_lote, name="pesquisa_empresas_em_lote")
]