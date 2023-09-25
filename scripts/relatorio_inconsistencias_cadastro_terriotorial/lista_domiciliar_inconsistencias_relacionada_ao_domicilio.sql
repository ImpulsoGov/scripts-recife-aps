SELECT
    tdtl.ds_tipo_logradouro ,
    tfcd.no_logradouro,
    tfcd.nu_num_logradouro,
    tfcd.no_complemento,
    tfcd.no_bairro,
    tfcd.nu_telefone_residencia,
    tfcd.co_seq_fat_cad_domiciliar,
    tfcd.nu_micro_area,
    tde.no_equipe,
    tfft.nu_prontuario,
    tde.nu_ine,
    tfcp.nu_cns,
    tfcp.no_social_cidadao,
    tfcp.no_cidadao,
    tfft.st_resp_com_fci_no_territorio,
    tfft.st_resp_declarado_no_fci,
    tfft.st_resp_outro_fcd_mais_atual,
    unidadevinculada.nu_cnes ,
    unidadevinculada.no_unidade_saude,
    equipevinculada.nu_ine,case 
    	when (
                tfft.st_resp_com_fci_no_territorio = 0 AND tfft.st_familia_fcd_mudouse = 0
                AND NOT EXISTS (
                    SELECT 1
                    FROM tb_fat_cidadao_territorio outroterritorio
                    WHERE
                        tfft.co_fat_cidadao_pec = outroterritorio.co_fat_cidadao_pec
                    AND
                        outroterritorio.st_vivo = 0
                ))
          then 2
          else 
          	case 
          		when (
	                tfft.st_resp_declarado_no_fci = 0
	                AND tfft.st_resp_com_fci_no_territorio = 1
	                AND tfft.st_familia_fcd_mudouse = 0
	                AND COALESCE(tfct.st_mudou_se, 0) = 0
	                AND COALESCE(tfct.st_vivo, 1) = 1
           			)
           		then 3
           		when  (
		                tfft.st_resp_outro_fcd_mais_atual = 1
		                AND COALESCE(tfct.st_mudou_se, 0) = 0
		                AND COALESCE(tfct.st_vivo, 1) = 1
		            )
           		then 4
           		else null
          	end
    end inconsisstencia_tipo,
    case 
    	when (
                tfft.st_resp_com_fci_no_territorio = 0 AND tfft.st_familia_fcd_mudouse = 0
                AND NOT EXISTS (
                    SELECT 1
                    FROM tb_fat_cidadao_territorio outroterritorio
                    WHERE
                        tfft.co_fat_cidadao_pec = outroterritorio.co_fat_cidadao_pec
                    AND
                        outroterritorio.st_vivo = 0
                ))
          then 'Responsável sem cadastro individual no território'
          else 
          	case 
          		when (
	                tfft.st_resp_declarado_no_fci = 0
	                AND tfft.st_resp_com_fci_no_territorio = 1
	                AND tfft.st_familia_fcd_mudouse = 0
	                AND COALESCE(tfct.st_mudou_se, 0) = 0
	                AND COALESCE(tfct.st_vivo, 1) = 1
           			)
           		then 'Responsável não declarado no cadastro individual'
           		when  (
		                tfft.st_resp_outro_fcd_mais_atual = 1
		                AND COALESCE(tfct.st_mudou_se, 0) = 0
		                AND COALESCE(tfct.st_vivo, 1) = 1
		            )
           		then 'Responsável em outro domicílio mais atual'
           		else null
          	end
    end inconsisstencia_classificacao
from tb_fat_familia_territorio tfft 
join tb_dim_unidade_saude tdus on tdus.co_seq_dim_unidade_saude = tfft.co_dim_unidade_saude
join tb_dim_equipe tde on tde.co_seq_dim_equipe = tfft.co_dim_equipe
join tb_fat_cad_domiciliar tfcd ON tfcd.co_seq_fat_cad_domiciliar = tfft.co_fat_cad_domiciliar
LEFT JOIN tb_fat_cidadao_territorio tfct  ON tfft.co_fat_cidadao_territorio = tfct.co_seq_fat_cidadao_territorio
LEFT JOIN tb_fat_cidadao_pec tfcp  ON tfct.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
LEFT JOIN  tb_dim_unidade_saude unidadevinculada on unidadevinculada.co_seq_dim_unidade_saude = tfcp.co_dim_unidade_saude_vinc
LEFT JOIN tb_dim_equipe equipevinculada ON equipevinculada.co_seq_dim_equipe = tfcp.co_dim_equipe_vinc
LEFT JOIN tb_dim_tempo tdt  ON tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
LEFT JOIN tb_dim_tipo_imovel tdti  on tdti.co_seq_dim_tipo_imovel = tfcd.co_dim_tipo_imovel
LEFT join tb_dim_tipo_logradouro tdtl on tdtl.co_seq_dim_tipo_logradouro = tfcd.co_dim_tipo_logradouro 
WHERE
    tfft.st_familia_consistente = 0
AND
    (
        (
            --:checkResponsavelCadastrado = 1
             (
                tfft.st_resp_com_fci_no_territorio = 0
                AND tfft.st_familia_fcd_mudouse = 0
                AND NOT EXISTS (
                    SELECT 1
                    FROM tb_fat_cidadao_territorio outroterritorio
                    WHERE
                        tfft.co_fat_cidadao_pec = outroterritorio.co_fat_cidadao_pec
                    AND
                        outroterritorio.st_vivo = 0
                )
            )
        )
        OR
        (
            --:checkResponsavelDeclarado = 1
             (
                tfft.st_resp_declarado_no_fci = 0
                AND tfft.st_resp_com_fci_no_territorio = 1
                AND tfft.st_familia_fcd_mudouse = 0
                AND COALESCE(tfct.st_mudou_se, 0) = 0
                AND COALESCE(tfct.st_vivo, 1) = 1
            )
        )
        OR
        (
            --:checkResponsavelUnico = 1
             (
                tfft.st_resp_outro_fcd_mais_atual = 1
                AND COALESCE(tfct.st_mudou_se, 0) = 0
                AND COALESCE(tfct.st_vivo, 1) = 1
            )
        )
    )
ORDER BY
    tde.nu_ine ASC,
    tfcd.nu_micro_area ASC,
    tfcd.co_seq_fat_cad_domiciliar ASC;