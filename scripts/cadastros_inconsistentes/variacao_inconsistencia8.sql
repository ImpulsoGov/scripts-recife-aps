CREATE OR REPLACE TABLE `aps_recife_dados_modelados.esus_cadastros_variacao_inconsistencia_tipo8`
AS
WITH 
  total_inconsistencias8_inicio_fim AS (
    SELECT
      tb1.unidade_saude_cnes,
      tb1.unidade_saude_nome,
      tb1.equipe_ine,
      tb1.equipe_nome,
      SUM(CASE WHEN tb1.relatorio_data = '2023-10-05' THEN tb1.total_inconsistencias ELSE 0 END) AS total_inconsistencias8_inicio,
      SUM(CASE 
            WHEN tb1.relatorio_data = (
                                        SELECT 
                                          tb1.relatorio_data
                                        FROM 
                                          `aps_recife_dados_modelados.esus_cadastros_inconsistentes_total_por_equipe` tb1
                                        ORDER BY 
                                          tb1.relatorio_data DESC
                                        LIMIT 1
                                      ) 
            THEN tb1.total_inconsistencias 
            ELSE 0 
          END) AS total_inconsistencias8_final,
      CAST(AVG(tb2.total_profissionais) AS INT64) as total_profissionais
    FROM
      `aps_recife_dados_modelados.esus_cadastros_inconsistentes_total_por_equipe` tb1
    LEFT JOIN `aps_recife_dados_modelados.scnes_total_profissionais_por_equipe` tb2 on tb1.equipe_ine = tb2.equipe_ine 
    WHERE
      tb1.inconsistencia_tipo = 8
    GROUP BY
      tb1.unidade_saude_cnes,
      tb1.unidade_saude_nome,
      tb1.equipe_ine,
      tb1.equipe_nome
    ORDER BY
      tb1.unidade_saude_cnes,
      tb1.unidade_saude_nome,
      tb1.equipe_ine,
      tb1.equipe_nome
  ),
  variacao_inconsistencias AS (
    SELECT 
        tb1.*,
        (total_inconsistencias8_final - (CASE WHEN total_inconsistencias8_inicio = 0 THEN 1 ELSE total_inconsistencias8_inicio END)) / (CASE WHEN total_inconsistencias8_inicio = 0 THEN 1 ELSE total_inconsistencias8_inicio END) as variacao_inconsistencias8
    FROM total_inconsistencias8_inicio_fim tb1
  )
  SELECT 
      tb1.unidade_saude_cnes,
      tb1.unidade_saude_nome,
      tb1.equipe_ine,
      tb1.equipe_nome,
      total_inconsistencias8_inicio,
      total_inconsistencias8_final,
      variacao_inconsistencias8 as variacao_inconsistencias8_media,
      total_profissionais,
      CASE 
        WHEN tb1.equipe_ine in ('0000153338','0000153591','0000153559','0000153575','0000154598','0000155993')
        THEN '1ª Grupo de Trabalho'
        ELSE 'Não ativado'
      END ativacao_equipe
  FROM variacao_inconsistencias tb1

