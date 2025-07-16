-- ============================================================================
-- CONSULTA COMPLETA DE EMPRESA POR CNPJ
-- Receita Federal - Dados Públicos CNPJ
-- ============================================================================

-- Parâmetros para a consulta:
-- @cnpj_input: CNPJ completo (14 dígitos) ou CNPJ básico (8 dígitos)
-- Exemplo: '11222333000181' ou '11222333'

-- ============================================================================
-- 1. DADOS DA EMPRESA (Informações básicas)
-- ============================================================================
SELECT 
    -- Identificação
    e.cnpj_basico,
    e.razao_social,
    e.natureza_juridica,
    nj.descricao as natureza_juridica_descricao,
    e.qualificacao_responsavel,
    qr.descricao as qualificacao_responsavel_descricao,
    e.capital_social,
    e.porte_empresa,
    e.ente_federativo_responsavel,
    
    -- Informações adicionais
    CASE 
        WHEN e.porte_empresa = '01' THEN 'Microempresa'
        WHEN e.porte_empresa = '03' THEN 'Empresa de Pequeno Porte'
        WHEN e.porte_empresa = '05' THEN 'Demais'
        ELSE 'Não informado'
    END as porte_empresa_descricao

FROM empresa e
LEFT JOIN natureza nj ON e.natureza_juridica = nj.codigo
LEFT JOIN qualificacao qr ON e.qualificacao_responsavel = qr.codigo
WHERE e.cnpj_basico = CASE 
    WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
    WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
    ELSE :cnpj_input
END;

-- ============================================================================
-- 2. ESTABELECIMENTOS (Filiais e Matriz)
-- ============================================================================
SELECT 
    -- Identificação completa
    est.cnpj_basico,
    est.cnpj_ordem,
    est.cnpj_dv,
    CONCAT(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) as cnpj_completo,
    
    -- Tipo de estabelecimento
    CASE 
        WHEN est.cnpj_ordem = '0001' THEN 'MATRIZ'
        ELSE 'FILIAL'
    END as tipo_estabelecimento,
    
    -- Identificação comercial
    est.nome_fantasia,
    est.situacao_cadastral,
    CASE 
        WHEN est.situacao_cadastral = '01' THEN 'NULA'
        WHEN est.situacao_cadastral = '02' THEN 'ATIVA'
        WHEN est.situacao_cadastral = '03' THEN 'SUSPENSA'
        WHEN est.situacao_cadastral = '04' THEN 'INAPTA'
        WHEN est.situacao_cadastral = '08' THEN 'BAIXADA'
        ELSE 'SITUAÇÃO DESCONHECIDA'
    END as situacao_cadastral_descricao,
    
    est.data_situacao_cadastral,
    est.motivo_situacao_cadastral,
    m.descricao as motivo_situacao_descricao,
    
    -- Endereço
    est.nome_cidade_exterior,
    est.pais,
    p.descricao as pais_descricao,
    est.data_inicio_atividade,
    est.cnae_fiscal_principal,
    cnae_prin.descricao as cnae_principal_descricao,
    est.cnae_fiscal_secundaria,
    
    -- CNAEs secundários formatados
    CASE 
        WHEN est.cnae_fiscal_secundaria IS NOT NULL AND est.cnae_fiscal_secundaria != '' THEN
            (SELECT string_agg(c.codigo || ' - ' || c.descricao, ', ' ORDER BY c.codigo)
             FROM unnest(string_to_array(est.cnae_fiscal_secundaria, ',')) AS sec_cnae(codigo)
             LEFT JOIN cnae c ON c.codigo = trim(sec_cnae.codigo)
             WHERE trim(sec_cnae.codigo) != '')
        ELSE NULL
    END as cnaes_secundarios_descricao,
    
    -- Endereço nacional
    est.tipo_logradouro,
    est.logradouro,
    est.numero,
    est.complemento,
    est.bairro,
    est.cep,
    est.uf,
    est.municipio,
    mun.descricao as municipio_descricao,
    
    -- Contato
    est.ddd_1,
    est.telefone_1,
    est.ddd_2,
    est.telefone_2,
    est.ddd_fax,
    est.fax,
    est.correio_eletronico,
    
    -- Situação especial
    est.situacao_especial,
    est.data_situacao_especial

FROM estabelecimento est
LEFT JOIN motivo m ON est.motivo_situacao_cadastral = m.codigo
LEFT JOIN pais p ON est.pais = p.codigo
LEFT JOIN cnae cnae_prin ON est.cnae_fiscal_principal = cnae_prin.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo
WHERE est.cnpj_basico = CASE 
    WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
    WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
    ELSE :cnpj_input
END
ORDER BY est.cnpj_ordem;

-- ============================================================================
-- 3. SÓCIOS E RESPONSÁVEIS
-- ============================================================================
SELECT 
    s.cnpj_basico,
    s.identificador_socio,
    CASE 
        WHEN s.identificador_socio = '1' THEN 'PESSOA JURÍDICA'
        WHEN s.identificador_socio = '2' THEN 'PESSOA FÍSICA'
        WHEN s.identificador_socio = '3' THEN 'ESTRANGEIRO'
        ELSE 'TIPO DESCONHECIDO'
    END as tipo_socio,
    
    s.nome_socio,
    s.cnpj_cpf_socio,
    s.qualificacao_socio,
    q.descricao as qualificacao_socio_descricao,
    s.data_entrada_sociedade,
    s.pais,
    p.descricao as pais_descricao,
    s.representante_legal,
    s.nome_representante,
    s.qualificacao_representante,
    qr.descricao as qualificacao_representante_descricao,
    s.faixa_etaria,
    CASE 
        WHEN s.faixa_etaria = '1' THEN '0 a 12 anos'
        WHEN s.faixa_etaria = '2' THEN '13 a 20 anos'
        WHEN s.faixa_etaria = '3' THEN '21 a 30 anos'
        WHEN s.faixa_etaria = '4' THEN '31 a 40 anos'
        WHEN s.faixa_etaria = '5' THEN '41 a 50 anos'
        WHEN s.faixa_etaria = '6' THEN '51 a 80 anos'
        WHEN s.faixa_etaria = '8' THEN 'Maior de 80 anos'
        WHEN s.faixa_etaria = '0' THEN 'Não se aplica'
        ELSE 'Não informado'
    END as faixa_etaria_descricao

FROM socios s
LEFT JOIN qualificacao q ON s.qualificacao_socio = q.codigo
LEFT JOIN pais p ON s.pais = p.codigo
LEFT JOIN qualificacao qr ON s.qualificacao_representante = qr.codigo
WHERE s.cnpj_basico = CASE 
    WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
    WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
    ELSE :cnpj_input
END
ORDER BY s.nome_socio;

-- ============================================================================
-- 4. SIMPLES NACIONAL
-- ============================================================================
SELECT 
    sim.cnpj_basico,
    sim.opcao_pelo_simples,
    CASE 
        WHEN sim.opcao_pelo_simples = 'S' THEN 'SIM'
        WHEN sim.opcao_pelo_simples = 'N' THEN 'NÃO'
        ELSE 'NÃO INFORMADO'
    END as opcao_simples_descricao,
    
    sim.data_opcao_simples,
    sim.data_exclusao_simples,
    sim.opcao_mei,
    CASE 
        WHEN sim.opcao_mei = 'S' THEN 'SIM'
        WHEN sim.opcao_mei = 'N' THEN 'NÃO'
        ELSE 'NÃO INFORMADO'
    END as opcao_mei_descricao,
    
    sim.data_opcao_mei,
    sim.data_exclusao_mei

FROM simples sim
WHERE sim.cnpj_basico = CASE 
    WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
    WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
    ELSE :cnpj_input
END;

-- ============================================================================
-- CONSULTA UNIFICADA - TODOS OS DADOS EM UMA ÚNICA QUERY
-- ============================================================================
WITH empresa_info AS (
    SELECT 
        e.cnpj_basico,
        e.razao_social,
        e.natureza_juridica,
        nj.descricao as natureza_juridica_descricao,
        e.qualificacao_responsavel,
        qr.descricao as qualificacao_responsavel_descricao,
        e.capital_social,
        e.porte_empresa,
        CASE 
            WHEN e.porte_empresa = '01' THEN 'Microempresa'
            WHEN e.porte_empresa = '03' THEN 'Empresa de Pequeno Porte'
            WHEN e.porte_empresa = '05' THEN 'Demais'
            ELSE 'Não informado'
        END as porte_empresa_descricao,
        e.ente_federativo_responsavel
    FROM empresa e
    LEFT JOIN natureza nj ON e.natureza_juridica = nj.codigo
    LEFT JOIN qualificacao qr ON e.qualificacao_responsavel = qr.codigo
    WHERE e.cnpj_basico = CASE 
        WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
        WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
        ELSE :cnpj_input
    END
),
estabelecimentos_info AS (
    SELECT 
        est.cnpj_basico,
        json_agg(
            json_build_object(
                'cnpj_completo', CONCAT(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv),
                'tipo', CASE WHEN est.cnpj_ordem = '0001' THEN 'MATRIZ' ELSE 'FILIAL' END,
                'nome_fantasia', est.nome_fantasia,
                'situacao_cadastral', est.situacao_cadastral,
                'data_situacao_cadastral', est.data_situacao_cadastral,
                'endereco', CONCAT(
                    COALESCE(est.tipo_logradouro, ''), ' ',
                    COALESCE(est.logradouro, ''), ', ',
                    COALESCE(est.numero, ''), ' ',
                    COALESCE(est.bairro, ''), ' - ',
                    COALESCE(mun.descricao, ''), '/',
                    COALESCE(est.uf, ''), ' ',
                    COALESCE(est.cep, '')
                ),
                'telefone', CONCAT(
                    COALESCE(est.ddd_1, ''), 
                    COALESCE(est.telefone_1, '')
                ),
                'email', est.correio_eletronico,
                'cnae_principal', cnae_prin.descricao,
                'data_inicio_atividade', est.data_inicio_atividade
            )
            ORDER BY est.cnpj_ordem
        ) as estabelecimentos
    FROM estabelecimento est
    LEFT JOIN municipio mun ON est.municipio = mun.codigo
    LEFT JOIN cnae cnae_prin ON est.cnae_fiscal_principal = cnae_prin.codigo
    WHERE est.cnpj_basico = CASE 
        WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
        WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
        ELSE :cnpj_input
    END
    GROUP BY est.cnpj_basico
),
socios_info AS (
    SELECT 
        s.cnpj_basico,
        json_agg(
            json_build_object(
                'nome', s.nome_socio,
                'tipo', CASE 
                    WHEN s.identificador_socio = '1' THEN 'PESSOA JURÍDICA'
                    WHEN s.identificador_socio = '2' THEN 'PESSOA FÍSICA'
                    WHEN s.identificador_socio = '3' THEN 'ESTRANGEIRO'
                    ELSE 'TIPO DESCONHECIDO'
                END,
                'qualificacao', q.descricao,
                'data_entrada', s.data_entrada_sociedade,
                'cpf_cnpj', s.cnpj_cpf_socio
            )
            ORDER BY s.nome_socio
        ) as socios
    FROM socios s
    LEFT JOIN qualificacao q ON s.qualificacao_socio = q.codigo
    WHERE s.cnpj_basico = CASE 
        WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
        WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
        ELSE :cnpj_input
    END
    GROUP BY s.cnpj_basico
),
simples_info AS (
    SELECT 
        sim.cnpj_basico,
        json_build_object(
            'opcao_simples', CASE 
                WHEN sim.opcao_pelo_simples = 'S' THEN 'SIM'
                WHEN sim.opcao_pelo_simples = 'N' THEN 'NÃO'
                ELSE 'NÃO INFORMADO'
            END,
            'data_opcao_simples', sim.data_opcao_simples,
            'data_exclusao_simples', sim.data_exclusao_simples,
            'opcao_mei', CASE 
                WHEN sim.opcao_mei = 'S' THEN 'SIM'
                WHEN sim.opcao_mei = 'N' THEN 'NÃO'
                ELSE 'NÃO INFORMADO'
            END,
            'data_opcao_mei', sim.data_opcao_mei,
            'data_exclusao_mei', sim.data_exclusao_mei
        ) as simples_nacional
    FROM simples sim
    WHERE sim.cnpj_basico = CASE 
        WHEN LENGTH(:cnpj_input) = 14 THEN SUBSTRING(:cnpj_input, 1, 8)
        WHEN LENGTH(:cnpj_input) = 8 THEN :cnpj_input
        ELSE :cnpj_input
    END
)
SELECT 
    ei.*,
    est.estabelecimentos,
    soc.socios,
    sim.simples_nacional,
    
    -- Resumo executivo
    (SELECT COUNT(*) FROM estabelecimento WHERE cnpj_basico = ei.cnpj_basico) as total_estabelecimentos,
    (SELECT COUNT(*) FROM socios WHERE cnpj_basico = ei.cnpj_basico) as total_socios,
    
    -- Data da consulta
    CURRENT_TIMESTAMP as data_consulta

FROM empresa_info ei
LEFT JOIN estabelecimentos_info est ON ei.cnpj_basico = est.cnpj_basico
LEFT JOIN socios_info soc ON ei.cnpj_basico = soc.cnpj_basico
LEFT JOIN simples_info sim ON ei.cnpj_basico = sim.cnpj_basico;