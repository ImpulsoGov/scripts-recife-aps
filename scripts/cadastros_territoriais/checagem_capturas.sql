WITH TabelasDesejadas AS (
  -- Lista de tabelas desejadas
  SELECT 'tb_dim_unidade_saude' AS tabela_nome
  UNION ALL SELECT 'tb_dim_cbo'
  UNION ALL SELECT 'tb_dim_municipio'
  UNION ALL SELECT 'tb_dim_equipe'
  UNION ALL SELECT 'tb_dim_profissional'
  UNION ALL SELECT 'tb_dim_tempo'
  UNION ALL SELECT 'tb_fat_cidadao_territorio'
  UNION ALL SELECT 'tb_fat_cidadao_pec'
  UNION ALL SELECT 'tb_fat_familia_territorio'
  UNION ALL SELECT 'tb_fat_cad_individual'
  UNION ALL SELECT 'tb_cds_cad_individual'
)
SELECT
    COUNT(DISTINCT tb2.tabela_nome) = 11 as captura_realizada
FROM TabelasDesejadas tb1
LEFT JOIN `aps_recife_dados_modelados.esus_etl_monitoramento` tb2
  ON tb1.tabela_nome = tb2.tabela_nome
  AND DATE(tb2.data_operacao) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
WHERE
  tb2.tabela_nome IS NOT NULL;


