CREATE OR REPLACE TABLE `aps_recife_camada_ouro.esus_cadastros_territorio`
as
SELECT 
            tfct.co_seq_fat_cidadao_territorio,
            tfct.co_fat_familia_territorio,
            tfcp.co_seq_fat_cidadao_pec,
            tfct.co_fat_cad_individual,
            tfctresp.co_fat_cad_individual as co_fat_cad_individual_responsavel_familiar,
            tfctresp.co_seq_fat_cidadao_territorio as co_seq_fat_cidadao_territorio_responsavel_familiar,
            distrito.distrito_ordem,
            distrito.distrito_nome,
            unidadasaude.nu_cnes as unidade_saude_cnes,
            unidadasaude.no_unidade_saude as unidade_saude_nome,
            equipesaude.no_equipe as equipe_nome,
            equipesaude.nu_ine as equipe_ine,
            tfct.nu_micro_area as micro_area,
            tfcp.nu_cns as cidadao_cns,
            tfcp.nu_cpf_cidadao as cidadao_cpf,
            tfcpresp.no_cidadao AS responsavel_nome,
            tfcpresp.nu_cns AS responsavel_cns,
            tfcp.no_cidadao as cidadao_nome,
            tfcp.no_social_cidadao as cidadao_nome_social,
            tempo.dt_registro as cidadao_data_nascimento,
            case 
              when tfct.st_responsavel = 0
              then false
              when tfct.st_responsavel = 1
              then true
              else null
            end cidadao_responsavel_familiar,
            case 
              when tfct.st_vivo = 0
              then false
              when tfct.st_vivo = 1
              then true
              else null
            end cidadao_vivo,
            case 
              when tfct.st_mudou_se = 0
              then false
              when tfct.st_mudou_se = 1
              then true
              else null
            end cidadao_mudou_se,
            case 
              when tfct.st_cns_null = 0
              then false
              when tfct.st_cns_null = 1
              then true
              else null
            end cidadao_com_cns_nulo,
            case 
              when tfct.st_cidadao_consistente = 0
              then false
              when tfct.st_cidadao_consistente = 1
              then true
              else null
            end cidadao_cadastro_consistente,
            CASE 
            WHEN tfct.st_cidadao_consistente = 0 or tfctresp.st_cidadao_consistente = 0 then 'Sim' 
            ELSE 'Não'
            END cadastro_inconsistente,
            case 
              when tfct.st_responsavel_informado = 0
              then false
              when tfct.st_responsavel_informado = 1
              then true
              else null
            end responsavel_familiar_informado,
            case 
              when tfft.st_responsavel_vivo = 0
              then false
              when tfft.st_responsavel_vivo = 1
              then true
              else null
            end responsavel_familiar_vivo,
            case 
              when tfct.st_responsavel_com_fci = 0
              then false
              when tfct.st_responsavel_com_fci = 1
              then true
              else null
            end responsavel_familiar_com_fci,
            case 
              when tfft.st_responsavel_ainda_reside = 0
              then false
              when tfft.st_responsavel_ainda_reside = 1
              then true
              else null
            end responsavel_familiar_ainda_reside,
            case 
              when tfctresp.st_mudou_se = 0
              then false
              when tfctresp.st_mudou_se = 1
              then true
              else null
            end responsavel_familiar_mudou_se,
            case 
              when tfctresp.st_cidadao_consistente = 0
              then false
              when tfctresp.st_cidadao_consistente = 1
              then true
              else null
            end responsavel_familiar_cadastro_consistente,
            unidadasaudevinc.nu_cnes AS unidade_vinculada_cnes,
            unidadasaudevinc.no_unidade_saude AS unidade_vinculada_nome,
            equipesaudevinc.nu_ine AS equipe_vinculada_ine,
            equipesaudevinc.no_equipe AS equipe_vinculada_nome
           FROM aps_recife_dados_brutos.tb_fat_cidadao_pec tfcp
             JOIN aps_recife_dados_brutos.tb_fat_cidadao_territorio tfct ON tfcp.co_seq_fat_cidadao_pec = tfct.co_fat_cidadao_pec
             LEFT JOIN aps_recife_dados_brutos.tb_dim_unidade_saude unidadasaude ON unidadasaude.co_seq_dim_unidade_saude = tfct.co_dim_unidade_saude
             LEFT JOIN aps_recife_dados_brutos.tb_dim_equipe equipesaude ON equipesaude.co_seq_dim_equipe = tfct.co_dim_equipe
             LEFT JOIN aps_recife_dados_brutos.tb_dim_tempo tempo ON tfcp.co_dim_tempo_nascimento = tempo.co_seq_dim_tempo
             LEFT JOIN aps_recife_dados_brutos.tb_fat_cidadao_territorio tfctresp ON tfct.co_fat_ciddo_terrtrio_resp = tfctresp.co_seq_fat_cidadao_territorio
             LEFT JOIN aps_recife_dados_brutos.tb_fat_cidadao_pec tfcpresp ON tfctresp.co_fat_cidadao_pec = tfcpresp.co_seq_fat_cidadao_pec
             LEFT JOIN aps_recife_dados_brutos.tb_fat_familia_territorio tfft ON tfct.co_fat_familia_territorio = tfft.co_seq_fat_familia_territorio
             LEFT JOIN aps_recife_dados_brutos.tb_dim_unidade_saude unidadasaudevinc ON tfcp.co_dim_unidade_saude_vinc = unidadasaudevinc.co_seq_dim_unidade_saude
             LEFT JOIN aps_recife_dados_brutos.tb_dim_equipe equipesaudevinc ON tfcp.co_dim_equipe_vinc = equipesaudevinc.co_seq_dim_equipe
             LEFT JOIN aps_recife_camada_prata.tb_dim_distrito_territorial distrito ON equipesaude.nu_ine = distrito.nu_ine
