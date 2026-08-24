from django.db import models

class NaturezaJuridica(models.Model):
    codigo = models.CharField(max_length=4, unique=True, primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        verbose_name = "Natureza Jurídica"
        verbose_name_plural = "Naturezas Jurídicas"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class Cnae(models.Model):
    codigo = models.CharField(max_length=7, unique=True, primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        verbose_name = "CNAE"
        verbose_name_plural = "CNAEs"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class Municipio(models.Model):
    codigo = models.CharField(max_length=7, unique=True, primary_key=True)
    nome = models.CharField(max_length=150)
    uf = models.CharField(max_length=2, default="")

    class Meta:
        verbose_name = "Município"
        verbose_name_plural = "Municípios"
        ordering = ["nome", ]
        indexes = [
            models.Index(fields=["nome",]),
        ]

    def __str__(self):
        return f"{self.nome}"

class Empresa(models.Model):
    VIABILIDADE_PRIMARIA = "Primaria"
    VIABILIDADE_SECUNDARIA = "Secundaria"

    cnpj = models.CharField(
        max_length=18,
        unique=True,
    )

    data_inicio_atividades = models.DateField(
        null=True,
        blank=True,
    )

    natureza_juridica = models.ForeignKey(
        NaturezaJuridica,
        on_delete=models.PROTECT,
        related_name="empresas",
        null=True,
        blank=True,
    )

    cnae_fiscal = models.ForeignKey(
        Cnae,
        on_delete=models.PROTECT,
        related_name="empresas_principal",
        null=True,
        blank=True,
    )

    cnaes_secundarios = models.ManyToManyField(
        Cnae,
        related_name="empresas_secundarias",
        blank=True,
    )

    razao_social = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    nome_fantasia = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    matriz_filial = models.CharField(
        max_length=20,
        null=True,
        blank=True,
    )

    decisor = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    situacao_cadastral = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        db_index=True,
    )

    correio_eletronico = models.EmailField(
        max_length=255,
        null=True,
        blank=True,
    )

    logradouro = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    numero = models.CharField(
        max_length=20,
        null=True,
        blank=True,
    )

    complemento = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    bairro = models.CharField(
        max_length=150,
        null=True,
        blank=True,
    )

    cep = models.CharField(
        max_length=8,
        null=True,
        blank=True,
        db_index=True,
    )

    municipio = models.ForeignKey(
        Municipio,
        on_delete=models.PROTECT,
        related_name="empresas",
        null=True,
        blank=True,
    )

    cpf = models.CharField(
        max_length=11,
        null=True,
        blank=True,
        db_index=True,
    )

    mei_nome_mei = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    telefone_receita_1 = models.CharField(
        max_length=20,
        null=True,
        blank=True,
    )

    telefone_receita_2 = models.CharField(
        max_length=20,
        null=True,
        blank=True,
    )

    telefone_receita_3 = models.CharField(
        max_length=20,
        null=True,
        blank=True,
    )

    viabilidade = models.CharField(max_length=len(VIABILIDADE_PRIMARIA)+10, choices=(
        ("", ""),
        (VIABILIDADE_PRIMARIA, VIABILIDADE_PRIMARIA),
        (VIABILIDADE_SECUNDARIA, VIABILIDADE_SECUNDARIA)
        ),
        null=True,
        blank=True,
    )

    class Meta:
        verbose_name = "Empresa"
        verbose_name_plural = "Empresas"
        indexes = [
            models.Index(fields=["razao_social"]),
            models.Index(fields=["nome_fantasia"]),
            models.Index(fields=["municipio"]),
            models.Index(fields=["cnae_fiscal"]),
            models.Index(fields=["natureza_juridica"]),
            models.Index(fields=["viabilidade"]),
        ]

    def __str__(self):
        return f"{self.cnpj} - {self.razao_social or 'Sem razão social'}"


