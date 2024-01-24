CREATE OR REPLACE TABLE `aps_recife_camada_ouro.esus_cadastros_inconsistentes`
as 
WITH    
    cad_individual_recente AS (
        SELECT 
            tfci.co_fat_cidadao_pec, 
            max(tfci.co_seq_fat_cad_individual) as co_seq_fat_cad_individual_recente
		FROM aps_recife_dados_brutos.tb_fat_cad_individual tfci 
		GROUP BY tfci.co_fat_cidadao_pec
    ),
    relaciona_cad_individual AS (
        SELECT 
            lcct.co_seq_fat_cidadao_territorio,
            lcct.co_fat_familia_territorio,
            lcct.co_seq_fat_cidadao_pec,			
            lcct.co_fat_cad_individual,			
            lcct.co_fat_cad_individual_responsavel_familiar,			
            lcct.co_seq_fat_cidadao_territorio_responsavel_familiar,			
            lcct.distrito_ordem,			
            lcct.distrito_nome,			
            lcct.unidade_saude_cnes,			
            lcct.unidade_saude_nome,			
            lcct.equipe_nome,			
            lcct.equipe_ine,			
            lcct.micro_area,			
            lcct.cidadao_cns,
            CASE
                WHEN lcct.cidadao_cpf is not NULL THEN
                LPAD(CAST(lcct.cidadao_cpf AS STRING), 11, '0')
                ELSE CAST(lcct.cidadao_cpf AS STRING)
            END AS cidadao_cpf,	
            lcct.responsavel_nome,			
            lcct.responsavel_cns,			
            lcct.cidadao_nome,			
            lcct.cidadao_nome_social,	
            lcct.cidadao_data_nascimento,			
            lcct.cidadao_responsavel_familiar,				
            lcct.cidadao_vivo,				
            lcct.cidadao_mudou_se,				
            lcct.cidadao_com_cns_nulo,				
            lcct.cidadao_cadastro_consistente,				
            lcct.cadastro_inconsistente,			
            lcct.responsavel_familiar_informado,				
            lcct.responsavel_familiar_vivo,				
            lcct.responsavel_familiar_com_fci,				
            lcct.responsavel_familiar_ainda_reside,				
            lcct.responsavel_familiar_mudou_se,				
            lcct.responsavel_familiar_cadastro_consistente,				
            lcct.unidade_vinculada_cnes,			
            lcct.unidade_vinculada_nome,			
            lcct.equipe_vinculada_ine,			
            lcct.equipe_vinculada_nome,	
            CASE 
                WHEN lcct.cidadao_nome_social IS NOT null THEN CONCAT(lcct.cidadao_nome_social,' (',lcct.cidadao_nome,')')
                ELSE lcct.cidadao_nome
            END cidadao_nome_painel,
            CASE 
                WHEN tfci.nu_cns = '0              ' THEN null
                ELSE tfci.nu_cns
            END cidadao_cns_fci,
            CASE 
                WHEN tfci.nu_cpf_cidadao = '0          ' THEN null
                ELSE tfci.nu_cpf_cidadao
            END cidadao_cpf_fci,
            CASE 
                WHEN tfci.nu_cns not in ('0              ') THEN tfci.nu_cns
                WHEN tfci.nu_cns = '0              ' and tfci.nu_cpf_cidadao not in ('0          ') THEN tfci.nu_cpf_cidadao
                WHEN tfci.nu_cns = '0              ' and tfci.nu_cpf_cidadao = '0          ' THEN null
            END cidadao_cns_cpf_fci,
            CASE 
                WHEN tfci.nu_cpf_responsavel is null THEN tfci.nu_cpf_responsavel
                ELSE tfci.nu_cpf_responsavel
            END responsavel_cpf_fci,
            tfci.nu_cns_responsavel AS responsavel_cns_fci,
            CASE 
                WHEN tfci.nu_cns_responsavel is not null THEN tfci.nu_cpf_responsavel
                WHEN tfci.nu_cns_responsavel is null and tfci.nu_cpf_responsavel is not null THEN tfci.nu_cpf_responsavel
                WHEN tfci.nu_cns_responsavel is null and tfci.nu_cpf_responsavel is null THEN null
            END responsavel_cns_cpf_fci,
            sexo_fci.ds_sexo as cidadao_sexo_fci,
            tcci.nu_celular_cidadao AS cidadao_celular_fci,
            tcci.no_mae_cidadao AS mae_nome_fci,
            tcci.no_pai_cidadao AS pai_nome_fci,
            nacionalidade_fci.no_identificador AS cidadao_nacionalidade_fci,
            municipio_fci.no_municipio AS cidadao_municipio_fci,
            uf_fci.sg_uf as cidadao_uf_fci,
            profissional_fci.no_profissional AS profissional_fci,
            cbo_fci.nu_cbo as profissional_cbo_fci,
            cbo_fci.no_cbo as profissional_cbo_descricao_fci,
            unidade_fci.nu_cnes AS unidade_saude_cnes_fci,
            equipe_fci.nu_ine AS equipe_ine_fci,
            tcci.dt_cad_individual AS data_ultima_atualizacao_fci
        FROM aps_recife_camada_ouro.esus_cadastros_territorio lcct
        LEFT JOIN cad_individual_recente fci on lcct.co_seq_fat_cidadao_pec = fci.co_fat_cidadao_pec
        LEFT JOIN aps_recife_dados_brutos.tb_fat_cad_individual tfci  on fci.co_seq_fat_cad_individual_recente = tfci.co_seq_fat_cad_individual
        LEFT JOIN aps_recife_dados_brutos.tb_cds_cad_individual tcci on tfci.nu_uuid_ficha = tcci.co_unico_ficha 
        LEFT JOIN aps_recife_dados_brutos.tb_dim_profissional profissional_fci ON tfci.co_dim_profissional = profissional_fci.co_seq_dim_profissional
        LEFT JOIN aps_recife_dados_brutos.tb_dim_cbo cbo_fci ON tfci.co_dim_cbo = cbo_fci.co_seq_dim_cbo
        LEFT JOIN aps_recife_dados_brutos.tb_dim_nacionalidade nacionalidade_fci ON tfci.co_dim_nacionalidade = nacionalidade_fci.co_seq_dim_nacionalidade
        LEFT JOIN aps_recife_dados_brutos.tb_dim_sexo sexo_fci ON tfci.co_dim_sexo = sexo_fci.co_seq_dim_sexo
        LEFT JOIN aps_recife_dados_brutos.tb_dim_unidade_saude unidade_fci ON tfci.co_dim_unidade_saude = unidade_fci.co_seq_dim_unidade_saude
        LEFT JOIN aps_recife_dados_brutos.tb_dim_equipe equipe_fci ON tfci.co_dim_equipe = equipe_fci.co_seq_dim_equipe
        LEFT JOIN aps_recife_dados_brutos.tb_dim_municipio municipio_fci ON tfci.co_dim_municipio_cidadao = municipio_fci.co_seq_dim_municipio
        LEFT JOIN aps_recife_dados_brutos.tb_dim_uf uf_fci ON municipio_fci.co_dim_uf = uf_fci.co_seq_dim_uf
        WHERE 
              -- filtra cadastros não consistentes para o cadastro do cidadão ou do seu responsável
              (lcct.cidadao_cadastro_consistente IS FALSE OR lcct.responsavel_familiar_cadastro_consistente  IS FALSE) 
            AND 
                -- check responsável mudou-se
                (lcct.co_seq_fat_cidadao_territorio_responsavel_familiar IS NOT NULL 
                AND lcct.responsavel_familiar_mudou_se  IS TRUE 
                AND lcct.cidadao_mudou_se  IS FALSE 
                AND lcct.cidadao_responsavel_familiar IS FALSE 
                AND lcct.cidadao_vivo  IS TRUE 
                AND lcct.responsavel_familiar_vivo  IS TRUE)
            OR 
                -- check responsável vivo
                (lcct.co_seq_fat_cidadao_territorio_responsavel_familiar IS NOT NULL 
                AND lcct.responsavel_familiar_vivo  IS FALSE 
                AND lcct.cidadao_responsavel_familiar IS FALSE 
                AND lcct.cidadao_vivo  IS TRUE 
                AND lcct.cidadao_mudou_se  IS FALSE) 
            OR  
                -- check responsável informado
                (lcct.responsavel_familiar_informado  IS FALSE 
                AND lcct.cidadao_responsavel_familiar IS FALSE 
                AND lcct.cidadao_vivo  IS TRUE 
                AND lcct.cidadao_mudou_se  IS FALSE) 
            OR 
                -- check sem vínculo com domicílio
                (lcct.co_fat_familia_territorio IS NULL 
                AND lcct.cidadao_vivo  IS TRUE 
                AND lcct.cidadao_mudou_se  IS FALSE 
                AND lcct.co_fat_cad_individual IS NOT NULL 
                AND lcct.responsavel_familiar_informado  IS TRUE)
                ),
    identifica_inconsistencias AS (
        SELECT
            *,
                CASE
                    WHEN responsavel_familiar_informado IS FALSE THEN 7
                    WHEN co_fat_familia_territorio IS NULL THEN 8
                    ELSE
                    CASE
                        WHEN responsavel_familiar_ainda_reside IS FALSE THEN 5
                        WHEN responsavel_familiar_vivo IS FALSE THEN 6
                        ELSE 0
                    END
                END AS inconsistencia_tipo,
                CASE
                    WHEN responsavel_familiar_informado IS FALSE THEN 0
                    WHEN co_fat_familia_territorio IS NULL THEN 
                    CASE
                        WHEN responsavel_familiar_com_fci IS FALSE THEN 1
                        WHEN cidadao_responsavel_familiar IS TRUE AND cidadao_cns_cpf_fci IS NULL THEN 4
                        WHEN cidadao_responsavel_familiar IS TRUE THEN 2
                        WHEN cidadao_responsavel_familiar IS FALSE THEN 3
                        WHEN cidadao_com_cns_nulo IS TRUE THEN 4
                        ELSE NULL
                    END
                    ELSE
                    CASE
                        WHEN responsavel_familiar_ainda_reside IS FALSE THEN NULL
                        WHEN responsavel_familiar_vivo IS FALSE THEN NULL
                        ELSE NULL
                    END
                END AS inconsistencia_sub_tipo,
                CASE
                    WHEN responsavel_familiar_informado IS FALSE THEN 1
                    WHEN co_fat_familia_territorio IS NULL THEN 1
                    ELSE
                    CASE
                        WHEN responsavel_familiar_ainda_reside IS FALSE THEN 1
                        WHEN responsavel_familiar_vivo IS FALSE THEN 1
                        ELSE 0
                    END
                END AS total_inconsistencias
           FROM relaciona_cad_individual
    )
SELECT 
      *
FROM identifica_inconsistencias
