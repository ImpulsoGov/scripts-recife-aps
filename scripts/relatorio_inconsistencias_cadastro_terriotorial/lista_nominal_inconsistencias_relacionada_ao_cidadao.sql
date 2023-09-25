WITH query_vinculacoes AS (
         SELECT unidadasaude.nu_cnes,
            unidadasaude.no_unidade_saude,
            equipesaude.nu_ine,
            tfct.nu_micro_area,
            equipesaude.no_equipe,
            tfcp.co_seq_fat_cidadao_pec,
            tfcp.nu_cns,
            tfcpresp.no_cidadao AS responsavel_nome,
            tfcpresp.nu_cns AS responsavelnucns,
            tfcp.no_cidadao,
            tfcp.no_social_cidadao,
            tempo.dt_registro,
            tfct.co_fat_cad_individual,
            tfct.co_fat_familia_territorio,
            tfct.co_seq_fat_cidadao_territorio,
            tfct.st_responsavel,
            tfft.st_responsavel_vivo,
            tfct.st_vivo AS stcidadaovivo,
            tfct.st_mudou_se AS stcidadaomudouse,
            tfct.st_responsavel_informado,
            tfct.st_responsavel_com_fci,
            tfct.st_cns_null,
            tfft.st_responsavel_ainda_reside,
            unidadasaudevinc.nu_cnes AS unidade_vinculada_cnes,
            unidadasaudevinc.no_unidade_saude AS unidade_vinculada_nome,
            equipesaudevinc.nu_ine AS equipe_vinculada_ine,
            equipesaudevinc.no_equipe AS equipe_vinculada_nome
           FROM tb_fat_cidadao_pec tfcp
             JOIN tb_fat_cidadao_territorio tfct ON tfcp.co_seq_fat_cidadao_pec = tfct.co_fat_cidadao_pec
             JOIN tb_dim_unidade_saude unidadasaude ON unidadasaude.co_seq_dim_unidade_saude = tfct.co_dim_unidade_saude
             JOIN tb_dim_equipe equipesaude ON equipesaude.co_seq_dim_equipe = tfct.co_dim_equipe
             JOIN tb_dim_tempo tempo ON tfcp.co_dim_tempo_nascimento = tempo.co_seq_dim_tempo
             LEFT JOIN tb_fat_cidadao_territorio tfctresp ON tfct.co_fat_ciddo_terrtrio_resp = tfctresp.co_seq_fat_cidadao_territorio
             LEFT JOIN tb_fat_cidadao_pec tfcpresp ON tfctresp.co_fat_cidadao_pec = tfcpresp.co_seq_fat_cidadao_pec
             LEFT JOIN tb_fat_familia_territorio tfft ON tfct.co_fat_familia_territorio = tfft.co_seq_fat_familia_territorio
             LEFT JOIN tb_dim_unidade_saude unidadasaudevinc ON tfcp.co_dim_unidade_saude_vinc = unidadasaudevinc.co_seq_dim_unidade_saude
             LEFT JOIN tb_dim_equipe equipesaudevinc ON tfcp.co_dim_equipe_vinc = equipesaudevinc.co_seq_dim_equipe
          WHERE (tfct.st_cidadao_consistente = 0 OR tfctresp.st_cidadao_consistente = 0) AND tfctresp.co_seq_fat_cidadao_territorio IS NOT NULL AND tfctresp.st_mudou_se = 1 AND tfct.st_mudou_se = 0 AND tfct.st_responsavel = 0 AND tfct.st_vivo = 1 AND tfctresp.st_vivo = 1 OR tfctresp.co_seq_fat_cidadao_territorio IS NOT NULL AND tfctresp.st_vivo = 0 AND tfct.st_responsavel = 0 AND tfct.st_vivo = 1 AND tfct.st_mudou_se = 0 OR tfct.st_responsavel_informado = 0 AND tfct.st_responsavel = 0 AND tfct.st_vivo = 1 AND tfct.st_mudou_se = 0 OR tfct.co_fat_familia_territorio IS NULL AND tfct.st_vivo = 1 AND tfct.st_mudou_se = 0 AND tfct.co_fat_cad_individual IS NOT NULL AND tfct.st_responsavel_informado = 1
        ), query_classificacao_inconsistencia AS (
         SELECT rel.nu_cnes AS unidade_cnes,
            rel.no_unidade_saude AS unidade_nome,
            rel.nu_ine AS equipe_ine,
            rel.no_equipe AS equipe_nome,
            rel.nu_micro_area AS micro_area,
            rel.co_seq_fat_cidadao_pec,
            rel.co_seq_fat_cidadao_territorio,
            rel.co_fat_familia_territorio,
            rel.no_cidadao AS cidadao_nome,
            rel.no_social_cidadao AS cidadao_nome_social,
            rel.dt_registro AS cidadao_data_nascimento,
            rel.nu_cns AS cidadao_cns,
            rel.responsavel_nome,
            rel.responsavelnucns AS responsavel_cns,
                CASE
                    WHEN rel.st_responsavel_com_fci = 1 THEN true
                    WHEN rel.st_responsavel_com_fci = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_com_fci,
                CASE
                    WHEN rel.st_cns_null = 1 THEN true
                    WHEN rel.st_cns_null = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_cns_null,
                CASE
                    WHEN rel.st_responsavel = 1 THEN true
                    WHEN rel.st_responsavel = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_domicilio,
                CASE
                    WHEN rel.st_responsavel_vivo = 1 THEN true
                    WHEN rel.st_responsavel_vivo = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_vivo,
                CASE
                    WHEN rel.stcidadaovivo = 1 THEN true
                    WHEN rel.stcidadaovivo = 0 THEN false
                    ELSE NULL::boolean
                END AS cidadao_vivo,
                CASE
                    WHEN rel.stcidadaomudouse = 1 THEN true
                    WHEN rel.stcidadaomudouse = 0 THEN false
                    ELSE NULL::boolean
                END AS cidadao_mudou_se,
                CASE
                    WHEN rel.st_responsavel = 1 THEN true
                    WHEN rel.st_responsavel = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_familiar,
                CASE
                    WHEN rel.st_responsavel_informado = 1 THEN true
                    WHEN rel.st_responsavel_informado = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_informado,
                CASE
                    WHEN rel.st_responsavel_ainda_reside = 1 THEN true
                    WHEN rel.st_responsavel_ainda_reside = 0 THEN false
                    ELSE NULL::boolean
                END AS responsavel_ainda_reside,
            rel.unidade_vinculada_cnes,
            rel.unidade_vinculada_nome,
            rel.equipe_vinculada_ine,
            rel.equipe_vinculada_nome,
                CASE
                    WHEN rel.st_responsavel_informado = 0 THEN 7
                    WHEN rel.co_fat_familia_territorio IS NULL THEN 8
                    ELSE
                    CASE
                        WHEN rel.st_responsavel_ainda_reside = 0 THEN 5
                        WHEN rel.st_responsavel_vivo = 0 THEN 6
                        ELSE 0
                    END
                END AS inconsistencia_tipo,
                CASE
                    WHEN rel.st_responsavel_informado = 0 THEN NULL::integer
                    WHEN rel.co_fat_familia_territorio IS NULL THEN
                    CASE
                        WHEN rel.st_responsavel_com_fci = 0 THEN 1
                        WHEN rel.st_responsavel = 1 AND rel.st_cns_null = 1 THEN 4
                        WHEN rel.st_responsavel = 1 THEN 2
                        WHEN rel.st_responsavel = 0 THEN 3
                        WHEN rel.st_cns_null = 1 THEN 4
                        ELSE NULL::integer
                    END
                    ELSE
                    CASE
                        WHEN rel.st_responsavel_ainda_reside = 0 THEN NULL::integer
                        WHEN rel.st_responsavel_vivo = 0 THEN NULL::integer
                        ELSE NULL::integer
                    END
                END AS inconsistencia_sub_tipo,
                CASE
                    WHEN rel.st_responsavel_informado = 0 THEN 'Responsável não informado'::text
                    WHEN rel.co_fat_familia_territorio IS NULL THEN
                    CASE
                        WHEN rel.st_responsavel_com_fci = 0 THEN 'Sem vínculo com o domicílio : Responsável sem FCI'::text
                        WHEN rel.st_responsavel = 1 AND rel.st_cns_null = 1 THEN 'Sem vínculo com o domicílio : Cidadão sem CNS informado'::text
                        WHEN rel.st_responsavel = 1 THEN 'Sem vínculo com o domicílio : Cidadão sem Domicílio'::text
                        WHEN rel.st_responsavel = 0 THEN 'Sem vínculo com o domicílio : Responsável declarado sem Domicílio'::text
                        ELSE NULL::text
                    END
                    ELSE
                    CASE
                        WHEN rel.st_responsavel_ainda_reside = 0 THEN 'Responsável com mudança de território'::text
                        WHEN rel.st_responsavel_vivo = 0 THEN 'Responsável com óbito no cadastro individual'::text
                        ELSE 'Sem registro de inconsistência'::text
                    END
                END AS inconsistencia_descricao,
                CASE
                    WHEN rel.st_responsavel_informado = 0 THEN 1
                    WHEN rel.co_fat_familia_territorio IS NULL THEN 1
                    ELSE
                    CASE
                        WHEN rel.st_responsavel_ainda_reside = 0 THEN 1
                        WHEN rel.st_responsavel_vivo = 0 THEN 1
                        ELSE 0
                    END
                END AS total_inconsistencias
           FROM query_vinculacoes rel
        ), query_identifica_duplicados AS (
         SELECT tb1.unidade_cnes,
            tb1.unidade_nome,
            tb1.equipe_ine,
            tb1.equipe_nome,
            tb1.micro_area,
            tb1.co_seq_fat_cidadao_pec,
            tb1.co_seq_fat_cidadao_territorio,
            tb1.co_fat_familia_territorio,
            tb1.cidadao_nome,
            tb1.cidadao_nome_social,
            tb1.cidadao_data_nascimento,
            tb1.cidadao_cns,
            tb1.responsavel_nome,
            tb1.responsavel_cns,
            tb1.responsavel_com_fci,
            tb1.responsavel_cns_null,
            tb1.responsavel_domicilio,
            tb1.responsavel_vivo,
            tb1.cidadao_vivo,
            tb1.cidadao_mudou_se,
            tb1.responsavel_familiar,
            tb1.responsavel_informado,
            tb1.responsavel_ainda_reside,
            tb1.unidade_vinculada_cnes,
            tb1.unidade_vinculada_nome,
            tb1.equipe_vinculada_ine,
            tb1.equipe_vinculada_nome,
            tb1.inconsistencia_tipo,
            tb1.inconsistencia_sub_tipo,
            tb1.inconsistencia_descricao,
            tb1.total_inconsistencias,
                CASE
                    WHEN (EXISTS ( SELECT 1
                       FROM query_classificacao_inconsistencia other
                      WHERE (other.cidadao_cns::text = tb1.cidadao_cns::text 
                      		OR other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec 
                      		OR (other.cidadao_nome::text = tb1.cidadao_nome::text AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento) 
                      		or (other.cidadao_nome::text = tb1.cidadao_nome::text AND other.responsavel_nome = tb1.responsavel_nome)
                      		)
                  		AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio)) THEN true
                    ELSE false
                END AS possivel_cadastro_duplicado
           FROM query_classificacao_inconsistencia tb1
          ORDER BY (
                CASE
                    WHEN (EXISTS ( SELECT 1
                       FROM query_classificacao_inconsistencia other
                      WHERE other.cidadao_nome::text = tb1.cidadao_nome::text AND other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec OR other.cidadao_nome::text = tb1.cidadao_nome::text AND other.cidadao_cns::text = tb1.cidadao_cns::text OR other.cidadao_nome::text = tb1.cidadao_nome::text AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento)) THEN tb1.cidadao_nome
                    ELSE tb1.cidadao_cns
                END)
        )
 SELECT 
    query_identifica_duplicados.co_seq_fat_cidadao_territorio,
    query_identifica_duplicados.co_seq_fat_cidadao_pec,
    query_identifica_duplicados.co_fat_familia_territorio,
    query_identifica_duplicados.cidadao_nome,
    query_identifica_duplicados.cidadao_nome_social,
    query_identifica_duplicados.cidadao_data_nascimento,
    query_identifica_duplicados.cidadao_cns,
    query_identifica_duplicados.cidadao_vivo,
    query_identifica_duplicados.cidadao_mudou_se,
    query_identifica_duplicados.unidade_cnes,
    query_identifica_duplicados.unidade_nome,
    query_identifica_duplicados.equipe_ine,
    query_identifica_duplicados.equipe_nome,
    query_identifica_duplicados.micro_area,
    query_identifica_duplicados.unidade_vinculada_cnes,
    query_identifica_duplicados.unidade_vinculada_nome,
    query_identifica_duplicados.equipe_vinculada_ine,
    query_identifica_duplicados.equipe_vinculada_nome,
    query_identifica_duplicados.responsavel_nome,
    query_identifica_duplicados.responsavel_cns,
    query_identifica_duplicados.responsavel_com_fci,
    query_identifica_duplicados.responsavel_cns_null,
    query_identifica_duplicados.responsavel_domicilio,
    query_identifica_duplicados.responsavel_vivo,
    query_identifica_duplicados.responsavel_familiar,
    query_identifica_duplicados.responsavel_informado,
    query_identifica_duplicados.responsavel_ainda_reside,
    query_identifica_duplicados.inconsistencia_tipo,
    query_identifica_duplicados.inconsistencia_sub_tipo,
    query_identifica_duplicados.inconsistencia_descricao,
    query_identifica_duplicados.total_inconsistencias,
    query_identifica_duplicados.possivel_cadastro_duplicado
   FROM query_identifica_duplicados