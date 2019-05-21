		#Consorcios
SELECT	c.id consorcio_id,
		c.name consorcio_nombre,
        c.chargePerDay consorcio_interes_primer_vencimiento,
        c.chargePerMonth consorcio_interes_segundo_vencimiento,
        c.modoPago consorcio_modo_pago,
        c.visibilidadGastos consorcio_visibilidad_gasto,
        c.tipo consorcio_tipo,
        c.usa_fondo consorcio_usa_fondo,
        c.soloMuestraCategorias consorcio_solo_muestra_cat,
        # Unidades
        u.id unidad_id,
        u.denomination unidad_denominacion,
        u.meters unidad_metros,
        u.type_id unidad_tipo,
        u.propietario_id unidad_propietario,
        # Expensas
        e.id expensa_id,
        e.inquilino_id unidad_inquilino,
        e.fecha_vencimiento_1 expensa_primer_vencimiento,
        e.fecha_vencimiento_2 expensa_segundo_vencimiento,
        e.monto expensa_monto,
        e.monto_total expensa_monto_total,
        e.monto_parcial expensa_monto_parcial,
        (e.monto / e.monto_total) * 100 epensa_proporcion,
        e.saldo_utilizado_en_pago expensa_saldo_utilizado,
        e.monto_descuento expensa_descuento,
        e.int_dia expensa_interes_primer_vencimiento,
        e.int_mes expensa_ineteres_segundo_vencimiento,
        e.mes expensa_mes,
        e.anio expensa_anio,
        e.version expensa_version,
		e.concepto_id expensa_concepto,
        # Pagos
        p.fecha pago_fecha,
        p.monto pago_monto,
        mp.nombre pago_metodo
FROM	consorcios c,
		unidades u,
        pago_metodo mp,
        expensa e
LEFT 
JOIN 	pago p 
ON		e.id = p.expensa_id
WHERE 	c.deleted = 0
AND		u.deleted = 0
AND 	e.deleted = 0
AND 	u.consorcio_id = c.id
AND 	e.unidad_id = u.id
AND 	e.concepto_id IN (1,10,599)
AND		mp.id = p.metodo
