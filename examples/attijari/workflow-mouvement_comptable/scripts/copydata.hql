SET tez.queue.name=${queueName};
use awb_${env};

INSERT INTO TABLE orc_mouvement_comptable
SELECT
  TRIM(SUBSTR(NUMERO_COMPTE, 1, 15)) AS numero_compte,
  TRIM(SUBSTR(NUMERO_COMPTE, 16, 1)) AS letr_cle,
  TRIM(BILAN) AS bilan,
  TRIM(LIBELLE) AS libelle,
  TRIM(LIB_ETENDU) AS lib_etendu,
  TRIM(SERVICE) AS service,
  CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(DT_VAL), 'yyyyMMdd'))) AS DATE) AS dt_val,
  CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(DT_OPE), 'yyyyMMdd'))) AS DATE) AS dt_ope,
  TRIM(CONCAT(SUBSTR(montant_signe, 16, 1),CONCAT(CONCAT(SUBSTR(montant_signe, 1, 13),'.'),SUBSTR(montant_signe, 14, 2)))) AS montant_signe,
  TRIM(SUBSTR(montant_signe, 16, 1)) AS signe,
  TRIM(extourne) AS extourne,
  TRIM(seq) AS seq,
  TRIM(cpt_anal) AS cpt_anal,
  CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(dt_tresor), 'yyyyMMdd'))) AS DATE) AS dt_tresor,
  CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(dt_compta), 'yyyyMMdd'))) AS DATE) AS dt_compta,
  TRIM(type_compta) AS type_compta,
  TRIM(libelle2) AS libelle2,
  TRIM(libelle3) AS libelle3,
  TRIM(dmd_avis) AS dmd_avis,
  TRIM(SUBSTR(filiere_num_oper, 1, 5)) AS filiere,
  TRIM(SUBSTR(filiere_num_oper, 6, 10)) AS num_oper,
  TRIM(ref_lettrage) AS ref_lettrage,
  TRIM(cd_oper) AS cd_oper,
  TRIM(nosrefr) AS nosrefr,
  TRIM(vosrefr) AS vosrefr,
  TRIM(cd_user) AS cd_user,
  TRIM(cd_visa) AS cd_visa,
  TRIM(SUBSTR(nom_appl_num_lot, 1, 5)) AS nom_appl,
  TRIM(SUBSTR(nom_appl_num_lot, 6, 1)) AS num_lot,
  TRIM(filler) AS filler,
  CURRENT_TIMESTAMP() AS `time`
FROM external_mouvement_comptable;