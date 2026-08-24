from django.urls import path
from .views import *

urlpatterns = [
    path('pesquisa_empresa', pesquisa_empresa, name="pesquisa_empresa"),
    path('pesquisa_empresas', pesquisa_empresas, name="pesquisa_empresas"),
]