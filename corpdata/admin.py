from django.contrib import admin
from corpdata.models import Telefones


class Telefones_Admin(admin.ModelAdmin):
    list_filter = ("telefone", "documento")

admin.site.register(Telefones, Telefones_Admin)
