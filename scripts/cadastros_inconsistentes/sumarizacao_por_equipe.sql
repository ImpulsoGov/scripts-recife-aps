
-- Extraindo o dia da semana da data corrente
DECLARE dia_corrente INT64;
SET dia_corrente = EXTRACT(DAYOFWEEK FROM CURRENT_DATE());

-- Verificando se é uma terça-feira (o código 3 corresponde a terça-feira)
IF dia_corrente = 3 THEN
  INSERT INTO `aps_recife_camada_ouro.esus_cadastros_inconsistentes_por_equipe_por_periodo`
      (     
        co_seq_inconsistencias_por_equipe,
        relatorio_data,
        distrito_nome, 
        unidade_saude_cnes, 
        unidade_saude_nome, 
        equipe_ine, 
        equipe_nome, 
        inconsistencia_tipo,
        inconsistencia_sub_tipo,
        total_inconsistencias,
        se_ativada,
        grupo_trabalho,
        total_profissionais,
        total_inconsistencias_anterior,
        variacao_percentual_inconsistencias
        )
  WITH 
        total_inconsistencias_por_equipe AS (
            SELECT 
                  GENERATE_UUID() as co_seq_inconsistencias_por_equipe,
                  cast(current_date() as string) as relatorio_data,
                  tb1.distrito_nome, 
                  tb1.unidade_saude_cnes, 
                  tb1.unidade_saude_nome, 
                  tb1.equipe_ine, 
                  tb1.equipe_nome, 
                  tb1.inconsistencia_tipo, 
                  tb1.inconsistencia_sub_tipo,
                  sum(total_inconsistencias) as total_inconsistencias,
                  tb2.se_ativada,
                  tb2.grupo_trabalho,
                  tb2.total_profissionais
            FROM `aps_recife_camada_ouro.esus_cadastros_inconsistentes` tb1
            LEFT JOIN `aps_recife_camada_ouro.equipes_ativadas` tb2 on tb1.equipe_ine = tb2.equipe_ine and tb1.unidade_saude_cnes = tb1.unidade_saude_cnes
            GROUP BY 
                  tb1.distrito_nome, 
                  tb1.unidade_saude_cnes, 
                  tb1.unidade_saude_nome, 
                  tb1.equipe_ine, 
                  tb1.equipe_nome, 
                  tb1.inconsistencia_tipo, 
                  tb1.inconsistencia_sub_tipo,
                  tb2.total_profissionais,
                  tb2.se_ativada,
                  tb2.grupo_trabalho
            ORDER BY 
                  tb1.distrito_nome, 
                  tb1.equipe_nome, 
                  tb1.inconsistencia_tipo
        ),
      une_relatorios AS (
            SELECT 
              tb1.*
            FROM total_inconsistencias_por_equipe tb1
            UNION ALL 
            SELECT 
              tb2.co_seq_inconsistencias_por_equipe,
              tb2.relatorio_data,
              tb2.distrito_nome, 
              tb2.unidade_saude_cnes, 
              tb2.unidade_saude_nome, 
              tb2.equipe_ine, 
              tb2.equipe_nome, 
              tb2.inconsistencia_tipo, 
              tb2.inconsistencia_sub_tipo,
              tb2.total_inconsistencias,
              tb2.se_ativada,
              tb2.grupo_trabalho,
              tb2.total_profissionais
            FROM `aps_recife_camada_ouro.esus_cadastros_inconsistentes_por_equipe_por_periodo` tb2
            WHERE tb2.relatorio_data = (SELECT relatorio_data
                                        FROM  `aps_recife_camada_ouro.esus_cadastros_inconsistentes_por_equipe_por_periodo`
                                        ORDER BY relatorio_data DESC LIMIT 1)
      ),
      total_inconsistencias_por_equipe_anterior AS (
          SELECT 
            tb1.*,
            LAG(tb1.total_inconsistencias) OVER (PARTITION BY tb1.unidade_saude_cnes, tb1.equipe_ine, tb1.inconsistencia_tipo, tb1.inconsistencia_sub_tipo ORDER BY tb1.relatorio_data) AS total_inconsistencias_anterior
          FROM une_relatorios tb1
      )
  SELECT 
      tb1.*,
      CASE
          WHEN total_inconsistencias IS NOT NULL AND total_inconsistencias_anterior <> 0
              THEN ROUND(((total_inconsistencias - total_inconsistencias_anterior) / total_inconsistencias_anterior),2)
          ELSE NULL
      END AS variacao_percentual_inconsistencias
  FROM total_inconsistencias_por_equipe_anterior tb1
  WHERE relatorio_data not in (SELECT relatorio_data
                              FROM  `aps_recife_camada_ouro.esus_cadastros_inconsistentes_por_equipe_por_periodo`
                              ORDER BY relatorio_data DESC LIMIT 1);
  
ELSE
  -- Consulta padrão caso não seja terça-feira
  SELECT 'Não é terça-feira';
END IF;