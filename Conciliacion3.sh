#!/bin/bash

######### Edito los parametros de conexion a la BD mysql ###########
MYSQL_HOME=/usr/bin
SQL_HOST="127.0.0.1"
SQL_USER="crontab_user"
SQL_PASS="crontab2019"
RUTA=~/Descargas
RUTA_OUTPUT=~/tmp
####################################################################


#Forma el nombre del archivo
#DIA=`date +%d`
#MES=`date +%m`
#YEAR=`date +%Y`
#HORA=`date +%H`
#MINUTO=`date +%M`
ARCHIVO_OUTPUT=OUT_$1
ARCHIVO_ESTADISTICA=EST_$1


cd $RUTA

### Se monta los parámetros de conexion
SQL_ARGS="--local-infile=1 -h $SQL_HOST -u $SQL_USER -P 5500 -p$SQL_PASS -e "

#Ejecutamos si el archivo de Conciliacion es PMC
if [[ ${1^^} =~ "PMC" ]]
then

#Truncamos Tabla
$MYSQL_HOME/mysql $SQL_ARGS "truncate table culqidb.tmp_pmc"


#Cargamos csv a Tabla
      $MYSQL_HOME/mysql $SQL_ARGS "
      LOAD DATA LOCAL INFILE \"$1\" 
      INTO TABLE culqidb.tmp_pmc
      CHARACTER SET latin1 
      FIELDS TERMINATED BY ',' 
      OPTIONALLY ENCLOSED BY '\"' 
      LINES TERMINATED BY '\n' 
      IGNORE 1 LINES;"

#Algunas Validaciones
$MYSQL_HOME/mysql $SQL_ARGS "delete from culqidb.tmp_pmc where nro_orden=''"
$MYSQL_HOME/mysql $SQL_ARGS "update culqidb.tmp_pmc set id=culqidb.rownum()"

#Exporta Resultados
$MYSQL_HOME/mysql $SQL_ARGS "select concat('\"',r.fecha_hora,'\"',',','\"',r.nro_orden,'\"',',','\"',r.seccode,'\"',',','\"',r.marca,'\"',',','\"',r.tarjeta,'\"',',',
'\"',r.monto,'\"',',','\"',r.abonado,'\"',',','\"',r.cod_auto,'\"',',','\"',r.dc,'\"',',',
'\"',r.cod_com,'\"',',','\"',r.comercio,'\"',',','\"',r.usuario_txn,'\"',',','\"',r.usuario_dp,'\"',',','\"',r.status_adquiriente,'\"',',','\"',r.id,'\"',',',
'\"',ifnull(r.nombre_comercial,''),'\"',',', '\"',ifnull(r.fecha_hora_creacion,''),'\"',',','\"',ifnull(r.exitosa,''),'\"',',','\"',ifnull(r.tarhab_email,''),'\"',',',
'\"',ifnull(r.hash_tarjeta,''),'\"',',','\"',ifnull(r.monto_autorizado,''),'\"',',','\"',ifnull(r.numero_tarjeta_enmascarada,''),'\"',',',
'\"',ifnull(r.nombre,'') ,'\"',',', '\"',ifnull(r.codigo_referencia,''),'\"',',', '\"',ifnull(r.codigo_unico_adq,''),'\"',',','\"',ifnull(tipo_bln,''),'\"',',','\"',ifnull(r.ref_id,''),'\"')
from(
select p.fecha_hora,p.nro_orden,p.seccode,p.marca,p.tarjeta,p.monto,p.abonado,p.cod_auto,p.dc,p.cod_com,p.comercio,p.usuario_txn,p.usuario_dp,
p.status_adquiriente,p.id,c.nombre_comercial , a.fecha_hora_creacion,a.exitosa,a.tarhab_email, a.hash_tarjeta,a.monto_autorizado, 
a.numero_tarjeta_enmascarada,et.nombre, a.codigo_referencia, a.codigo_unico_adq,case when wm.migration_date IS NOT NULL then 't0 (Evolution)'
when cconf.id IS NOT NULL then 'Ledger'
else 'T+n (Core)'  end tipo_bln,a.ref_id
from culqidb.tmp_pmc p
left join culqidb.autorizacion a on a.codigo_referencia=p.nro_orden
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_adquiriente ca on ca.comercio_id=c.id
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
left join culqidb.whitemarches wm on wm.codigo_comercio =c.codigo_comercio
left join culqidb.comercio_configuracion cconf on cconf.comercio_id =c.id and cconf.configuracion_id =11
where p.nro_orden <>''
group by a.codigo_referencia,a.ref_id,p.nro_orden,p.id
UNION 
select p2.fecha_hora,p2.nro_orden,p2.seccode,p2.marca,p2.tarjeta,p2.monto,p2.abonado,p2.cod_auto,p2.dc,p2.cod_com,p2.comercio,p2.usuario_txn,
p2.usuario_dp,p2.status_adquiriente,p2.id,'','','','','','','','','','','',''
from culqidb.tmp_pmc p2
where not  exists(
select 1 from  culqidb.tmp_pmc p3 where p2.id=p3.id)) r" > $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Elimina Lineas
sed -i '1,3d' $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Insertamos Cabeceras
sed -i "1i fecha_hora,nro_orden,seccode,marca,tarjeta,monto,abonado,cod_auto,dc,cod_com,comercio,usuario_txn,usuario_dp,status_adquiriente,id,nombre_comercio,fecha_hora_creacion,exitosa,tarhab_email,hash_tarjeta,monto_autorizado,numero_tarjeta_enmascarada,estado_culqi,codigo_referencia,codigo_unico_adq,tipo_bln,ref_id" $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Exporta Estadisticas
$MYSQL_HOME/mysql $SQL_ARGS "select 'Input: ', count(*) from culqidb.tmp_pmc
union all
select 'Output: ', count(*) from culqidb.tmp_pmc
union all
select 'No Encontrados: ', count(*) from 
(select * from (
select p.*,c.nombre_comercial as nombre_comercio, a.fecha_hora_creacion,a.exitosa,a.tarhab_email, a.hash_tarjeta,a.monto_autorizado, a.numero_tarjeta_enmascarada ,
et.nombre as estado_culqi, a.codigo_referencia, a.codigo_unico_adq
from culqidb.tmp_pmc p
left join culqidb.autorizacion a on a.codigo_referencia=p.nro_orden
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_adquiriente ca on ca.comercio_id=c.id
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
where p.nro_orden <>''
group by a.codigo_referencia,a.ref_id,p.nro_orden,p.id
UNION 
select p2.*,'','','','','','','','','',''
from culqidb.tmp_pmc p2
where not  exists(
select 1 from  culqidb.tmp_pmc p3 where p2.id=p3.id)
)r 
where r.estado_culqi='' or r.estado_culqi is null
)r2" > $RUTA_OUTPUT/$ARCHIVO_ESTADISTICA.log

elif [[ ${1^^} =~ "MPOS" ]]
then 

#Convertimos archivo a formato Unix
dos2unix $1

#Truncamos Tabla
$MYSQL_HOME/mysql $SQL_ARGS "truncate table culqidb.tmp_visa_mpos"


#Cargamos csv a Tabla
      $MYSQL_HOME/mysql $SQL_ARGS "
      LOAD DATA LOCAL INFILE \"$1\" 
      INTO TABLE culqidb.tmp_visa_mpos
      CHARACTER SET latin1 
      FIELDS TERMINATED BY ',' 
      OPTIONALLY ENCLOSED BY '\"' 
      LINES TERMINATED BY '\n' 
      IGNORE 1 LINES;"

#Algunas Validaciones
$MYSQL_HOME/mysql $SQL_ARGS "update culqidb.tmp_visa_mpos set id=culqidb.rownum()"      

#Exporta Resultados
$MYSQL_HOME/mysql $SQL_ARGS "select concat('\"',RUC,'\"',',','\"',Cod_Comercio,'\"',',','\"',Producto,'\"',',','\"',Moneda,'\"',',','\"',Fecha_operacion,'\"',',',
'\"',Tipo_operacion,'\"',',','\"',id_operacion,'\"',',','\"',Estado,'\"',',','\"',Motivo_venta_observada,'\"',',','\"',Monto,'\"',',',
'\"',N_serie_terminal,'\"',',','\"',Codigo_autorizacion,'\"',',','\"',N_referencia,'\"',',','\"',N_lote,'\"',',','\"',Monto_DCC,'\"',',',
'\"',DCC,'\"',',','\"',N_tarjeta,'\"',',','\"',Origen_tarjeta,'\"',',','\"',Tipo_tarjeta,'\"',',','\"',Marca_tarjeta,'\"',',','\"',Tipo_captura,'\"',
',','\"',Banco_emisor,'\"',',','\"',Numero_de_cuenta,'\"',',','\"',Banco_Pagador,'\"',',','\"',N_cuotas,'\"',',','\"',Trx_cuotas_sin_intereses,'\"',
',','\"',Nombre_del_programa,'\"',',','\"',ifnull(Importe_descontado,'-'),'\"',',','\"',id,'\"',',','\"',ifnull(comercio_id,''),'\"',',', '\"',ifnull(nombre_comercio,''),'\"',',', '\"',
ifnull(fecha_hora_creacion,''),'\"',',','\"',ifnull(exitosa,''),'\"',',','\"',ifnull(tarhab_email,''),'\"',',','\"',ifnull(hash_tarjeta,''),'\"',',','\"',
ifnull(monto_autorizado,''),'\"',',','\"',ifnull(numero_tarjeta_enmascarada,''),'\"',',','\"',ifnull(estado_culqi,''),'\"',',','\"',
ifnull(codigo_referencia,''),'\"',',','\"',ifnull(codigo_unico_adq,''),'\"',',','\"',ifnull(tipo_bln,''),'\"',',','\"',ifnull(ref_id,''),'\"') from(
select tp.RUC,tp.Cod_Comercio,tp.Producto,tp.Moneda,tp.Fecha_operacion,tp.Tipo_operacion,tp.id_operacion,tp.Estado,tp.Motivo_venta_observada,tp.Monto,tp.N_serie_terminal,
tp.Codigo_autorizacion,tp.N_referencia,tp.N_lote,tp.Monto_DCC,tp.DCC,tp.N_tarjeta,tp.Origen_tarjeta,tp.Tipo_tarjeta,tp.Marca_tarjeta,tp.Tipo_captura,tp.Banco_emisor,
tp.Numero_de_cuenta,tp.Banco_Pagador,tp.N_cuotas,tp.Trx_cuotas_sin_intereses,tp.Nombre_del_programa,tp.Importe_descontado,tp.id,c.id as comercio_id,
c.nombre_comercial as nombre_comercio, a.fecha_hora_creacion,a.exitosa,a.tarhab_email, a.hash_tarjeta,a.monto_autorizado, a.numero_tarjeta_enmascarada ,
et.nombre as estado_culqi, a.codigo_referencia, a.codigo_unico_adq, case when wm.migration_date IS NOT NULL then 't0 (Evolution)'
when cconf.id IS NOT NULL then 'Ledger'
else 'T+n (Core)'  end tipo_bln, a.ref_id
from culqidb.tmp_visa_mpos tp
left join culqidb.autorizacion a on a.codigo_unico_adq=tp.id_operacion
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_adquiriente ca on ca.comercio_id=c.id #and ca.codigo_comercio=tp.codigo
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
left join culqidb.whitemarches wm on wm.codigo_comercio =c.codigo_comercio
left join culqidb.comercio_configuracion cconf on cconf.comercio_id =c.id and cconf.configuracion_id =11
where tp.id_operacion <>''
group by a.codigo_referencia,a.ref_id,codigo_unico_adq,tp.id
UNION 
select tp2.RUC,tp2.Cod_Comercio,tp2.Producto,tp2.Moneda,tp2.Fecha_operacion,tp2.Tipo_operacion,tp2.id_operacion,tp2.Estado,tp2.Motivo_venta_observada,tp2.Monto,
tp2.N_serie_terminal,tp2.Codigo_autorizacion,tp2.N_referencia,tp2.N_lote,Monto_DCC,tp2.DCC,N_tarjeta,tp2.Origen_tarjeta,tp2.Tipo_tarjeta,tp2.Marca_tarjeta,
tp2.Tipo_captura,tp2.Banco_emisor,tp2.Numero_de_cuenta,tp2.Banco_Pagador,tp2.N_cuotas,tp2.Trx_cuotas_sin_intereses,tp2.Nombre_del_programa,tp2.Importe_descontado,tp2.id,
'','','','','','','','','','','','',''
from culqidb.tmp_visa_mpos tp2
where not  exists(
select 1 from  culqidb.tmp_visa_mpos tp3 where tp2.id=tp3.id))r" > $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Elimina Lineas
sed -i '1,3d' $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Insertamos Cabeceras
sed -i "1i RUC,Cod_Comercio,Producto,Moneda,Fecha_operacion,Tipo_operacion,id_operacion,Estado,Motivo_venta_observada,Monto,N_serie_terminal,Codigo_autorizacion,N_referencia,N_lote,Monto_DCC,DCC,N_tarjeta,Origen_tarjeta,Tipo_tarjeta,Marca_tarjeta,Tipo_captura,Banco_emisor,Numero_de_cuenta,Banco_Pagador,N_cuotas,Trx_cuotas_sin_intereses,Nombre_del_programa,Importe_descontado,id,comercio_id,nombre_comercio,fecha_hora_creacion,exitosa,tarhab_email,hash_tarjeta,monto_autorizado,numero_tarjeta_enmascarada,estado_culqi,codigo_referencia,codigo_unico_adq,tipo_bln,ref_id" $RUTA_OUTPUT/$ARCHIVO_OUTPUT



#Exporta Estadisticas
$MYSQL_HOME/mysql $SQL_ARGS "select 'Input: ', count(*) from culqidb.tmp_visa_mpos
union all
select 'Output: ', count(*) from culqidb.tmp_visa_mpos
union all
select 'No Encontrados: ', count(*)  
from culqidb.tmp_visa_mpos tp
left join culqidb.autorizacion a on a.codigo_unico_adq=tp.id_operacion
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_adquiriente ca on ca.comercio_id=c.id #and ca.codigo_comercio=tp.codigo
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
where tp.id_operacion<>''
and et.nombre is null or et.nombre=''
group by a.codigo_referencia,a.ref_id,codigo_unico_adq" > $RUTA_OUTPUT/$ARCHIVO_ESTADISTICA.log

elif [[ ${1^^} =~ "TRANSACCIONES" ]]
then 

#Convertimos archivo a formato Unix
dos2unix $1

#Truncamos Tabla
$MYSQL_HOME/mysql $SQL_ARGS "truncate table culqidb.tmp_visa_comercio"


#Cargamos csv a Tabla
      $MYSQL_HOME/mysql $SQL_ARGS "
      LOAD DATA LOCAL INFILE \"$1\" 
      INTO TABLE culqidb.tmp_visa_comercio
      CHARACTER SET latin1 
      FIELDS TERMINATED BY ';' 
      OPTIONALLY ENCLOSED BY '\"' 
      LINES TERMINATED BY '\n' 
      IGNORE 1 LINES;"

#Algunas Validaciones
$MYSQL_HOME/mysql $SQL_ARGS "update culqidb.tmp_visa_comercio set id_culqi=culqidb.rownum()"      

#Exporta Resultados
$MYSQL_HOME/mysql $SQL_ARGS "select concat('\"',r2.ruc,'\"',',','\"',r2.commerce_id,'\"',',','\"',r2.channel,'\"',',','\"',r2.id,'\"',',','\"',r2.trx_date,'\"',',','\"',
r2.shop_po_number,'\"',',','\"',r2.currency,'\"',',','\"',r2.order_amount,'\"',',','\"',r2.auth_payment,'\"',',','\"',r2.settled_amount,'\"',',','\"',
r2.bin,'\"',',','\"',r2.pan,'\"',',','\"',r2.brand,'\"',',','\"',r2.cardholder,'\"',',','\"',r2.cardholder_email,'\"',',','\"',r2.state,'\"',',','\"',
r2.authorization_code,'\"',',','\"',r2.authorization_message,'\"',',','\"',r2.confirmation_date,'\"',',','\"',r2.annulation_date,'\"',',','\"',
r2.annulation_reason,'\"',',','\"',r2.eci,'\"',',','\"',r2.xid,'\"',',','\"',r2.cavv,'\"',',','\"',dccAmount,'\"',',','\"',r2.dccExchangeRate,'\"',',','\"',
r2.panEnc,'\"',',','\"',r2.dccIndicator,'\"',',','\"',r2.registerFrequent,'\"',',','\"',r2.useFrequent,'\"',',','\"',r2.id_procesador,'\"',',','\"',
r2.brandActionCode,'\"',',','\"',r2.id_culqi,'\"',',','\"',ifnull(r2.nombre_comercio,''),'\"',',','\"',ifnull(r2.fecha_hora_creacion,''),'\"',',','\"',
ifnull(r2.exitosa,''),'\"',',','\"',ifnull(r2.tarhab_email,''),'\"',',','\"', ifnull(r2.hash_tarjeta,''),'\"',',','\"',ifnull(r2.monto_autorizado,''),'\"',',','\"',
ifnull(r2.numero_tarjeta_enmascarada,'') ,'\"',',','\"',ifnull(r2.estado_culqi,''),'\"',',','\"', ifnull(r2.codigo_referencia,''),'\"',',','\"', 
ifnull(r2.codigo_unico_adq,''), '\"',',','\"',ifnull(r2.signature,'') ,'\"',',','\"',ifnull(tipo_bln,''),'\"',',','\"',ifnull(r2.ref_id,''),'\"'
)
from(
select p.ruc,p.commerce_id,p.channel,p.id,p.trx_date,p.shop_po_number,p.currency,p.order_amount,p.auth_payment,p.settled_amount,p.bin,p.pan,p.brand,
p.cardholder,p.cardholder_email,p.state,p.authorization_code,p.authorization_message,p.confirmation_date,p.annulation_date,p.annulation_reason,
p.eci,p.xid,p.cavv,p.dccAmount,p.dccExchangeRate,p.panEnc,p.dccIndicator,p.registerFrequent,p.useFrequent,p.id_procesador,p.brandActionCode,
id_culqi,c.nombre_comercial as nombre_comercio, a.fecha_hora_creacion,a.exitosa,a.tarhab_email, a.hash_tarjeta,a.monto_autorizado, a.numero_tarjeta_enmascarada ,
et.nombre as estado_culqi, a.codigo_referencia, a.codigo_unico_adq,avva.signature,case when wm.migration_date IS NOT NULL then 't0 (Evolution)'
when cconf.id IS NOT NULL then 'Ledger'
else 'T+n (Core)'  end tipo_bln, a.ref_id
from culqidb.tmp_visa_comercio p 
left join culqidb.autorizacion a on a.codigo_unico_adq =p.id_procesador
and SUBSTRING(trx_date,1,10)=date(a.fecha_hora_creacion)
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_procesador_terminal cpt on a.comercio_procesador_terminal_id=cpt.id
left join culqidb.comercio_adquiriente ca on ca.id=cpt.comercio_adquiriente_id #and ca.adquiriente_id=4
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
left join culqidb.adq_vdp_v3_authorization avva on avva.autorizacion_id =a.id and a.exitosa=0
left join culqidb.whitemarches wm on wm.codigo_comercio =c.codigo_comercio
left join culqidb.comercio_configuracion cconf on cconf.comercio_id =c.id and cconf.configuracion_id =11
where ca.adquiriente_id=4
and cc.tipo_contrato_id =1
and p.id_procesador <>''
group by p.id_procesador,p.id
UNION 
select t.*,
'','','','','','','','','','','','',''
from culqidb.tmp_visa_comercio t
where not  exists(
select 1 from (select id_culqi
from culqidb.tmp_visa_comercio p 
left join culqidb.autorizacion a on a.codigo_unico_adq =p.id_procesador
and SUBSTRING(trx_date,1,10)=date(a.fecha_hora_creacion)
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_procesador_terminal cpt on a.comercio_procesador_terminal_id=cpt.id
left join culqidb.comercio_adquiriente ca on ca.id=cpt.comercio_adquiriente_id #and ca.adquiriente_id=4
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
left join culqidb.adq_vdp_v3_authorization avva on avva.autorizacion_id =a.id and a.exitosa=0
where ca.adquiriente_id=4
and cc.tipo_contrato_id =1
and p.id_procesador <>''
group by p.id_procesador,p.id_culqi) r  where t.id_culqi=r.id_culqi)
)r2" > $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Elimina Lineas
sed -i '1,2d' $RUTA_OUTPUT/$ARCHIVO_OUTPUT

#Insertamos Cabeceras
sed -i "1i ruc,commerce_id,channel,id,trx_date,shop_po_number,currency,order_amount,auth_payment,settled_amount,bin,pan,brand,cardholder,cardholder_email,state,authorization_code,authorization_message,confirmation_date,annulation_date,annulation_reason,eci,xid,cavv,dccAmount,dccExchangeRate,panEnc,dccIndicator,registerFrequent,useFrequent,id_procesador,brandActionCode,id_culqi,nombre_comercio,fecha_hora_creacion,exitosa,tarhab_email,hash_tarjeta,monto_autorizado,numero_tarjeta_enmascarada,estado_culqi,codigo_referencia,codigo_unico_adq,signature,tipo_bln,ref_id " $RUTA_OUTPUT/$ARCHIVO_OUTPUT



#Exporta Estadisticas
$MYSQL_HOME/mysql $SQL_ARGS "select 'Input: ', count(*) from culqidb.tmp_visa_comercio
union all
select 'Output: ', count(*) from culqidb.tmp_visa_comercio
union all
select 'No Encontrados: ', count(*)  
from culqidb.tmp_visa_comercio t
where not  exists(
select 1 from (select id_culqi
from culqidb.tmp_visa_comercio p 
left join culqidb.autorizacion a on a.codigo_unico_adq =p.id_procesador
and SUBSTRING(trx_date,1,10)=date(a.fecha_hora_creacion)
LEFT JOIN culqidb.transaccion t on a.id=t.autorizacion_id
LEFT JOIN culqidb.historial_transaccion ht ON t.historial_transaccion_actual_id= ht.id
LEFT JOIN culqidb.estado_transaccion et ON ht.estado_transaccion_id = et.id
LEFT JOIN culqidb.estado_transaccion_usuario etu ON et.estado_transaccion_usuario_id = etu.id
left join culqidb.comercio c on c.id=t.comercio_id
left join culqidb.comercio_procesador_terminal cpt on a.comercio_procesador_terminal_id=cpt.id
left join culqidb.comercio_adquiriente ca on ca.id=cpt.comercio_adquiriente_id #and ca.adquiriente_id=4
left join culqidb.comercio_contrato cc on cc.id=ca.comercio_contrato_id
where ca.adquiriente_id=4
and cc.tipo_contrato_id =1
and p.id_procesador <>''
group by p.id_procesador,p.id_culqi) r  where t.id_culqi=r.id_culqi)" > $RUTA_OUTPUT/$ARCHIVO_ESTADISTICA.log



fi
