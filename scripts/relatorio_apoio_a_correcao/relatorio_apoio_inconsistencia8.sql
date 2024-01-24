CREATE OR REPLACE TABLE `aps_recife_camada_ouro.esus_cadastros_inconsistentes_apoio_correcao`
as 
WITH    
    relatorio_cadastros_inconsistentes AS (
      SELECT *
      FROM  `aps_recife_camada_ouro.esus_cadastros_inconsistentes`
      WHERE 
            distrito_ordem in (1,2)
      AND   inconsistencia_tipo = 8
                ),
    identifica_inconsistencias AS (
        SELECT
            *,
                CASE
                    WHEN responsavel_familiar_informado IS FALSE THEN 'Responsável não informado'
                    WHEN co_fat_familia_territorio IS NULL THEN
                    CASE
                        WHEN responsavel_familiar_com_fci IS FALSE THEN 'Sem vínculo com o domicílio: Responsável sem FCI'
                        WHEN cidadao_responsavel_familiar IS TRUE AND cidadao_cns_cpf_fci IS NULL THEN 'Sem vínculo com o domicílio: Cidadão sem CNS informado'
                        WHEN cidadao_responsavel_familiar IS TRUE THEN 'Sem vínculo com o domicílio: Cidadão sem Domicílio'
                        WHEN cidadao_responsavel_familiar IS FALSE THEN 'Sem vínculo com o domicílio: Responsável declarado sem Domicílio'
                        ELSE NULL
                    END
                    ELSE
                    CASE
                        WHEN responsavel_familiar_ainda_reside IS FALSE THEN 'Responsável com mudança de território'
                        WHEN responsavel_familiar_vivo IS FALSE THEN 'Responsável com óbito no cadastro individual'
                        ELSE 'Sem registro de inconsistência'
                    END
                END AS inconsistencia_descricao,
                CASE
                    WHEN responsavel_familiar_informado IS FALSE THEN ''
                    WHEN co_fat_familia_territorio IS NULL THEN
                    CASE
                        WHEN responsavel_familiar_com_fci IS FALSE THEN CONCAT(
                                                                                'O responsável de CNS: ',responsavel_cns,
                                                                                ' não possui cadastro individual identificado sob o referido documento:','\n\n',
                                                                                '1) Verifique se o documento deste responsável está correto. Caso esteja incorreto, faça a correção;', '\n\n',
                                                                                '2) Caso o documento esteja correto, crie uma ficha de cadastro individual para esta pessoa, não se esquecendo de marcá-la como responsável familiar;', '\n\n',
                                                                                '3) Vincule esta pessoa a um domicílio como responsável.'
                                                                                )
                        WHEN cidadao_responsavel_familiar IS TRUE AND cidadao_com_cns_nulo  IS TRUE THEN ''
                        WHEN cidadao_responsavel_familiar IS TRUE THEN CONCAT(
                                                                                'O cidadão selecionado está marcado como responsável familiar. Contudo, não está vinculado como responsável familiar em nenhum domicílio:',
                                                                                '\n\n',
                                                                                '1) Verifique se este cadastro e a condição de responsável familiar está correta. Caso não esteja, faça as adequações;', '\n\n',
                                                                                '2) Caso os dados estejam corretos, vincule esta pessoa a um domicílio como responsável.'
                                                                                )
                        WHEN cidadao_responsavel_familiar IS FALSE THEN CONCAT(
                                                                                'O responsável familiar por este cadastro é: ',responsavel_nome,'/','CNS: ', responsavel_cns,
                                                                                '\n\n',
                                                                                '1) Verifique se o documento deste responsável está correto. Caso esteja incorreto, faça a correção;', 
                                                                                '\n\n',
                                                                                '2) Vincule este responsável a um domicílio como responsável familiar.'
                                                                                )
                        ELSE ''
                    END
                    ELSE
                    CASE
                        WHEN responsavel_familiar_ainda_reside IS FALSE THEN ''
                        WHEN responsavel_familiar_vivo IS FALSE THEN ''
                        ELSE ''
                    END
                END AS inconsistencia_correcao
           FROM relatorio_cadastros_inconsistentes
    ),
    sinaliza_possivel_duplicacao as 
         (SELECT
            *,
            case
              /* Quando CNS é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_cns = tb1.cidadao_cns )
                            AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio)
              then true
              /* Quando co_seq_fat_cidadao_pec é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec  )
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio) 
              then true
              /* Quando nome e data de nasicimento é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_nome = tb1.cidadao_nome AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento)
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio) 
              then true
              ELSE false
              END AS possivel_cadastro_duplicado,
            case
              /* Quando CNS é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_cns = tb1.cidadao_cns )
                            AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio
                      and other.cidadao_cadastro_consistente is true and other.responsavel_familiar_cadastro_consistente is true)
              then true
              /* Quando nome e data de nasicimento é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec  )
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio
                      and other.cidadao_cadastro_consistente is true and other.responsavel_familiar_cadastro_consistente is true) 
              then true
              /* Quando nome e data de nasicimento é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_nome = tb1.cidadao_nome AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento)
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio
                      and other.cidadao_cadastro_consistente is true and other.responsavel_familiar_cadastro_consistente is true) 
              then true
              ELSE false
              END AS possivel_cadastro_duplicado_consistente
           FROM identifica_inconsistencias tb1
           ),
        identifica_cadastro_duplicado as (
                select 
                *,
                case
                /* Quando CNS é igual */
                when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_cns = tb1.cidadao_cns )
                            AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio)
                then (select STRING_AGG(concat(
                                                'Nome: ',other.cidadao_nome,'\n',
                                                'Data de nascimento: ',other.cidadao_data_nascimento,'\n',
                                                'CNS: ',other.cidadao_cns,'\n',
                                                'Equipe: ',other.equipe_nome,'\n',
                                                'Cadastro inconsistente? ',other.cadastro_inconsistente
                                                ),
                                                '\n\n')
                        FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_cns = tb1.cidadao_cns )
                            AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio)
                /* Quando co_seq_fat_cidadao_pec é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec  )
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio) 
              then (select STRING_AGG(concat(
                                                'Nome: ',other.cidadao_nome,'\n',
                                                'Data de nascimento: ',other.cidadao_data_nascimento,'\n',
                                                'CNS: ',other.cidadao_cns,'\n',
                                                'Equipe: ',other.equipe_nome,'\n',
                                                'Cadastro inconsistente?: ',other.cadastro_inconsistente
                                                ),
                                                '\n\n')
                        FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.co_seq_fat_cidadao_pec = tb1.co_seq_fat_cidadao_pec ))
                /* Quando nome e data de nasicimento é igual */
              when EXISTS ( SELECT 1
                       FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_nome = tb1.cidadao_nome AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento)
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio) 
              then (select STRING_AGG(concat(
                                                'Nome: ',other.cidadao_nome,'\n',
                                                'Data de nascimento: ',other.cidadao_data_nascimento,'\n',
                                                'CNS: ',other.cidadao_cns,'\n',
                                                'Equipe: ',other.equipe_nome,'\n',
                                                'Cadastro inconsistente?: ',other.cadastro_inconsistente
                                                ),
                                                '\n\n')
                        FROM aps_recife_dados_modelados.esus_cadastros_territorio other
                      WHERE (other.cidadao_nome = tb1.cidadao_nome AND other.cidadao_data_nascimento = tb1.cidadao_data_nascimento)
                      AND other.co_seq_fat_cidadao_territorio <> tb1.co_seq_fat_cidadao_territorio) 
              ELSE null
              END AS cadastro_duplicado
                from sinaliza_possivel_duplicacao tb1
        )
        select 
            co_seq_fat_cidadao_territorio,
            co_fat_familia_territorio,
            co_seq_fat_cidadao_pec,			
            co_fat_cad_individual,			
            co_fat_cad_individual_responsavel_familiar,			
            co_seq_fat_cidadao_territorio_responsavel_familiar,			
            distrito_ordem,			
            distrito_nome,			
            unidade_saude_cnes,			
            unidade_saude_nome,			
            equipe_nome,			
            equipe_ine,			
            micro_area,			
            cidadao_cns,
            CASE
                WHEN LENGTH(CAST(cidadao_cpf as STRING)) = 11 THEN
                CONCAT(SUBSTR(CAST(cidadao_cpf as STRING), 1, 3), '.', SUBSTR(CAST(cidadao_cpf as STRING), 4, 3), '.', SUBSTR(CAST(cidadao_cpf as STRING), 7, 3), '-', SUBSTR(CAST(cidadao_cpf as STRING), 10, 2))
                ELSE
                CAST(cidadao_cpf as STRING)
            END AS cidadao_cpf,	
            responsavel_nome,			
            responsavel_cns,			
            cidadao_nome,			
            cidadao_nome_social,	
            CASE 
                WHEN cidadao_data_nascimento is not null 
                THEN CONCAT(SUBSTR(CAST(cidadao_data_nascimento as STRING), 9, 2), '/', SUBSTR(CAST(cidadao_data_nascimento as STRING), 6, 2), '/', SUBSTR(CAST(cidadao_data_nascimento as STRING), 1, 4))
                ELSE CAST(cidadao_data_nascimento as STRING)
            END cidadao_data_nascimento,			
            cidadao_responsavel_familiar,				
            cidadao_vivo,				
            cidadao_mudou_se,				
            cidadao_com_cns_nulo,				
            cidadao_cadastro_consistente,				
            cadastro_inconsistente,			
            responsavel_familiar_informado,				
            responsavel_familiar_vivo,				
            responsavel_familiar_com_fci,				
            responsavel_familiar_ainda_reside,				
            responsavel_familiar_mudou_se,				
            responsavel_familiar_cadastro_consistente,				
            unidade_vinculada_cnes,			
            unidade_vinculada_nome,			
            equipe_vinculada_ine,			
            equipe_vinculada_nome,			
            cidadao_nome_painel,
            cidadao_cns_fci,
            CASE
                WHEN LENGTH(CAST(cidadao_cpf_fci as STRING)) = 11 THEN
                CONCAT(SUBSTR(CAST(cidadao_cpf_fci as STRING), 1, 3), '.', SUBSTR(CAST(cidadao_cpf_fci as STRING), 4, 3), '.', SUBSTR(CAST(cidadao_cpf_fci as STRING), 7, 3), '-', SUBSTR(CAST(cidadao_cpf_fci as STRING), 10, 2))
                ELSE
                CAST(cidadao_cpf_fci as STRING)
            END AS cidadao_cpf_fci,	
            CASE
                WHEN LENGTH(CAST(cidadao_cns_cpf_fci as STRING)) = 11 THEN
                CONCAT(SUBSTR(CAST(cidadao_cns_cpf_fci as STRING), 1, 3), '.', SUBSTR(CAST(cidadao_cns_cpf_fci as STRING), 4, 3), '.', SUBSTR(CAST(cidadao_cns_cpf_fci as STRING), 7, 3), '-', SUBSTR(CAST(cidadao_cns_cpf_fci as STRING), 10, 2))
                ELSE
                CAST(cidadao_cns_cpf_fci as STRING)
            END AS cidadao_cns_cpf_fci,				
            responsavel_cns_fci,
            CASE
                WHEN LENGTH(CAST(responsavel_cpf_fci as STRING)) = 11 THEN
                CONCAT(SUBSTR(CAST(responsavel_cpf_fci as STRING), 1, 3), '.', SUBSTR(CAST(responsavel_cpf_fci as STRING), 4, 3), '.', SUBSTR(CAST(responsavel_cpf_fci as STRING), 7, 3), '-', SUBSTR(CAST(responsavel_cpf_fci as STRING), 10, 2))
                ELSE
                CAST(responsavel_cpf_fci as STRING)
            END AS responsavel_cpf_fci,		
            CASE
                WHEN LENGTH(CAST(responsavel_cns_cpf_fci as STRING)) = 11 THEN
                CONCAT(SUBSTR(CAST(responsavel_cns_cpf_fci as STRING), 1, 3), '.', SUBSTR(CAST(responsavel_cns_cpf_fci as STRING), 4, 3), '.', SUBSTR(CAST(responsavel_cns_cpf_fci as STRING), 7, 3), '-', SUBSTR(CAST(responsavel_cns_cpf_fci as STRING), 10, 2))
                ELSE
                CAST(responsavel_cns_cpf_fci as STRING)
            END AS responsavel_cns_cpf_fci,			
            cidadao_sexo_fci,
           CONCAT('(', SUBSTR(CAST(cidadao_celular_fci AS STRING), 1, 2), ') ',
            SUBSTR(CAST(cidadao_celular_fci AS STRING), 3, 5), '-', 
           SUBSTR(CAST(cidadao_celular_fci AS STRING), 8, 4)
            ) AS cidadao_celular_fci,
            mae_nome_fci,				
            pai_nome_fci,			
            cidadao_nacionalidade_fci,			
            cidadao_municipio_fci,			
            cidadao_uf_fci,			
            profissional_fci,			
            profissional_cbo_fci,			
            profissional_cbo_descricao_fci,			
            unidade_saude_cnes_fci,			
            equipe_ine_fci,
            data_ultima_atualizacao_fci,			
            inconsistencia_tipo,			
            inconsistencia_sub_tipo,			
            inconsistencia_descricao,			
            inconsistencia_correcao,			
            total_inconsistencias,			
            possivel_cadastro_duplicado,				
            possivel_cadastro_duplicado_consistente,				
            cadastro_duplicado
        from identifica_cadastro_duplicado 
