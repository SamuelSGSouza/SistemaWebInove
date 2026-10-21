from django.contrib import admin
from corpdata.models import Telefones


class Telefones_Admin(admin.ModelAdmin):
    pass

admin.site.register(Telefones, Telefones_Admin)
