SET @ID = vID;
SET @STATUS = (SELECT IF(COUNT(*) > 5000, 'Completo', 'Erro') FROM vBanco.vTabela WHERE atualizado_em >= DATE_ADD(CURDATE(), INTERVAL -1 DAY));
SET @REGISTROS = (SELECT COUNT(*) FROM vBanco.vTabela WHERE atualizado_em >= DATE_ADD(CURDATE(), INTERVAL -1 DAY));

INSERT INTO dba_db_adm.tb_log_atualizacao_hist (data, id_processo, evento, atualizacao, status, ultima_data, registros, inicio, fim, duracao, arquivo)
SELECT etl_data AS data, id as id_processo, nome as evento, atualizacao, status, data as ultima_data, registros, inicio, fim, duracao, arquivo
FROM dba_db_adm.tb_log_atualizacao 
WHERE id in (@ID);