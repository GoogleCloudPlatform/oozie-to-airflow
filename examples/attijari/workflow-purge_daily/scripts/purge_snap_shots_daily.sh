#!/bin/bash
# Réalisé par Amine SADIK le 10/04/2019 

kinit -kt /opt/datalake.keytab datalake@DATALAKE.PRD

TABLE=$1
TABLE1=$2

retention=$3
dateretention=$(date -d "-$retention days" +%Y-%m-%d)

echo -e "list_snapshots '${TABLE}-SnapShot.*'" | hbase shell -n  | grep -v " ${TABLE} " | grep "${TABLE}"  | while read -r line ; do
	datefichier=`echo $line | grep -oP '[\d]{4}-[\d]{2}-[\d]{2}'`
	echo $datefichier
	if [[ "$datefichier" < "$dateretention" || "$datefichier" == "$dateretention" ]] ; then 
	echo "delete_snapshot '$line'" | hbase shell
	echo " Snap '$line' supprimée " 
	fi 
done



#echo -e "list_snapshots '${TABLE1}-SnapShot.*'" | hbase shell -n  | grep -v " ${TABLE1} " | grep "${TABLE1}"  | while read -r line ; do
#        datefichier=`echo $line | grep -oP '[\d]{4}-[\d]{2}-[\d]{2}'`
#        echo $datefichier
#        if [[ "$datefichier" < "$dateretention" || "$datefichier" == "$dateretention" ]] ; then
#        echo "delete_snapshot '$line'" | hbase shell
#        echo " Snap '$line' supprimée "
#        fi
#done


