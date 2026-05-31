EXPECTED_TABLES = [
    "empresa",
    "estabelecimento",
    "socios",
    "simples",
    "cnae",
    "motivo",
    "municipio",
    "natureza",
    "pais",
    "qualificacao",
]

EXPECTED_INDEXES = [
    "empresa_cnpj",
    "estabelecimento_cnpj",
    "estabelecimento_cnpj_completo",
    "socios_cnpj",
    "simples_cnpj",
    "estabelecimento_situacao",
    "estabelecimento_municipio",
    "empresa_razao_social_trgm",
    "estabelecimento_nome_fantasia_trgm",
]
