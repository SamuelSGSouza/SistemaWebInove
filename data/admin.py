from django.contrib import admin
from .models import *


class Fase_Execucao_DB_Admin(admin.ModelAdmin):
    pass

admin.site.register(Fase_Execucao_DB, Fase_Execucao_DB_Admin)