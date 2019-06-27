# -*- coding: utf-8 -*-
"""
Created on Fri May 31 12:01:45 2019

@author: smaldonado
"""

import pandas as pd
def fix_wrong_dates(data):
    
    vencido_12_selection = ((data.expensa_primer_vencimiento == '0000-00-00') 
                            | (data.expensa_segundo_vencimiento == '0000-00-00')
                           | (data.expensa_primer_vencimiento.str.contains('-00'))
                           | (data.expensa_segundo_vencimiento.str.contains('-00'))) & (data.consorcio_modo_pago == 'mes_vencido') & (data.expensa_mes == 12)
    vencido_resto_selection = ((data.expensa_primer_vencimiento == '0000-00-00') | (data.expensa_segundo_vencimiento == '0000-00-00') | (data.expensa_primer_vencimiento.str.contains('-00'))
                           | (data.expensa_segundo_vencimiento.str.contains('-00'))) & (data.consorcio_modo_pago == 'mes_vencido') & (data.expensa_mes != 12)
    adelantado_selection = ((data.expensa_primer_vencimiento == '0000-00-00') | (data.expensa_segundo_vencimiento == '0000-00-00')| (data.expensa_primer_vencimiento.str.contains('-00'))
                           | (data.expensa_segundo_vencimiento.str.contains('-00'))) & (data.consorcio_modo_pago == 'mes_adelantado')
        

    #vencido_12_selection
    data.loc[vencido_12_selection, "expensa_primer_vencimiento"] = (data.loc[vencido_12_selection, "expensa_anio"].map(int) + 1).map(str) + "-01-10"
    data.loc[vencido_12_selection, "expensa_segundo_vencimiento"] = (data.loc[vencido_12_selection, "expensa_anio"].map(int) + 1).map(str) + "-01-20"

    #vencido_resto_selection
    data.loc[vencido_resto_selection, "expensa_primer_vencimiento"] = data.loc[vencido_resto_selection, "expensa_anio"] + "-" + (data.loc[vencido_resto_selection, "expensa_mes"].map(int) + 1).apply(lambda x: '{0:0>2}'.format(x)) + "-10"
    data.loc[vencido_resto_selection, "expensa_segundo_vencimiento"] = data.loc[vencido_resto_selection, "expensa_anio"] + "-" + (data.loc[vencido_resto_selection, "expensa_mes"].map(int) + 1).apply(lambda x: '{0:0>2}'.format(x)) + "-20"
    
    #adelantado_12_selection
    data.loc[adelantado_selection, "expensa_primer_vencimiento"] = data.loc[adelantado_selection, "expensa_anio"] + "-" + data.loc[adelantado_selection, "expensa_mes"].apply(lambda x: '{0:0>2}'.format(x)) + "-10"
    data.loc[adelantado_selection, "expensa_segundo_vencimiento"] = data.loc[adelantado_selection, "expensa_anio"] + "-" + data.loc[adelantado_selection, "expensa_mes"].apply(lambda x: '{0:0>2}'.format(x)) + "-20"    
    
    return data

def create_dataset():
    expensas_raw = pd.read_csv("../../data/raw/expensas_full.csv",
                            sep=",", 
                            na_values = "NULL",
                            dtype = {
                                "id": "object",
                                "unidad_id": "object",
                                "propietario_id": "object",
                                "inquilino_id": "object",
                                "consorcio_id": "object",
                                "consorcio_usa_fondo": "bool",
                                "consorcio_solo_muestra_cat": "bool",
                                "unidad_tipo":"object",
                                "expensa_concepto": "object",
                                "expensa_mes":"object",
                                "expensa_anio":"object"
                            })
    
    expensas = expensas_raw.loc[expensas_raw.consorcio_id != "1702",:] #Eliminacion Opera Ficticio
    expensas = fix_wrong_dates(expensas)
    #Eliminiacion de columnas sin variacion o repetidas
    expensas = expensas.drop(columns=["expensa_saldo_utilizado","expensa_descuento", "consorcio_interes_primer_vencimiento", "consorcio_interes_segundo_vencimiento"])
    
    
    #Eliminacion de expensas de carga de mora previa
    expensas.consorcio_fecha_creacion = pd.to_datetime(expensas.consorcio_fecha_creacion)
    expensas.expensa_primer_vencimiento = pd.to_datetime(expensas.expensa_primer_vencimiento)
    expensas = expensas.loc[expensas.consorcio_fecha_creacion < expensas.expensa_primer_vencimiento]    
    expensas = expensas.loc[expensas.expensa_proporcion <= 90]
    
    #Solucion de moras mal cargadas..
    expensas = expensas.loc[(expensas.expensa_monto > 0) & (expensas.expensa_monto_total > 0)]
    
    #Filtrado de fechas.
    expensas = expensas.loc[(expensas.expensa_anio >= "2016") & (expensas.expensa_anio < "2019")]
    
    expensas.pago_fecha = pd.to_datetime(expensas.pago_fecha, errors="coerce") 
    
    # Generacion fechas -Expensa fecha -Mes de Pago
    expensas.expensa_mes = expensas.expensa_mes.str.pad(width=2, side = "left", fillchar= "0")
    expensas["expensa_fecha"] = pd.to_datetime(expensas.expensa_anio + "-" + expensas.expensa_mes + "-01")
    expensas["expensa_mes_pago"] = pd.to_datetime(expensas.expensa_primer_vencimiento.dt.year.map(str) + '-'+ expensas.expensa_primer_vencimiento.dt.month.map(str) + '-01')
    
    # Generacion de "Target"
    expensas.loc[expensas.pago_fecha.isna() | (expensas.pago_fecha >= (expensas.expensa_mes_pago + pd.DateOffset(months=1))), "target"] = 1
    expensas.loc[expensas.target.isna(), "target"] = 0
    
    # Mes de pago anterior
    expensas["expensa_mes_pago_anterior"] = expensas.expensa_mes_pago - pd.DateOffset(months=1)
    
    # Simplificacion de metodo de pago
    expensas.loc[expensas.pago_metodo.isin(["Efectivo","Cheque"]),"pago_metodo"] = "Efec-Cheque"
    expensas.loc[expensas.pago_metodo.isin(["Rapipago", "Pago Facil"]),"pago_metodo"] = "EntePago"
    expensas.loc[expensas.pago_metodo.isin(["Transferencia", "Pago Mis Cuentas","Link Pagos","Bapropagos","Débito directo", "Débito Santander"]),"pago_metodo"] = "Internet"
    expensas.loc[expensas.pago_metodo.isin(["Nota de crédito", "Saldo a favor unidad"]),"pago_metodo"] = "Otro"
    expensas.loc[expensas.pago_metodo.isna() | expensas.target.map(bool),"pago_metodo"] = "Impago"
    
    
    # Adicion de informacion historica del consorcio
    resumen_consorcios = pd.read_csv("../../data/processed/resumen_consorcios.csv",
                                dtype = {
                                   "consorcio_id":"object"
                               },
                               parse_dates = ["expensa_mes_pago"])
    resumen_consorcios.columns = ['consorcio_id', 'consorcio_expensa_mes_pago', 'impagos', 'consorcio_cantidad_expensas',
       'cantidad_efectivo', 'consorcio_prop_impagos', 'consorcio_prop_efectivo']
    
    resumen_consorcios = resumen_consorcios[['consorcio_id', 'consorcio_expensa_mes_pago', 
                                             'consorcio_cantidad_expensas', 'consorcio_prop_impagos', 
                                             'consorcio_prop_efectivo']]
    
    expensas = pd.merge(expensas,
                resumen_consorcios,
                left_on=["expensa_mes_pago_anterior","consorcio_id"],
                right_on=["consorcio_expensa_mes_pago", "consorcio_id"],
                how = "left",
                suffixes = ("", "_consorcio"))
    
    # Adicion de información histórica de unidades
    resumen_unidades = pd.read_csv("../../data/processed/resumen_unidades.csv", 
                               dtype = {
                                   "unidad_id":"object"
                               },
                               parse_dates = ["expensa_mes_pago"])
    resumen_unidades.columns = ['unidad_id', 'unidad_expensa_mes_pago', 'unidad_prop_impagos']
    
    expensas = pd.merge(expensas,
            resumen_unidades,
            left_on=["expensa_mes_pago_anterior","unidad_id"],
            right_on=["unidad_expensa_mes_pago", "unidad_id"],
            how = "left",
            suffixes = ("", "_unidad"))
    
    # Seleccion de columnas relevantes
    expensas = expensas [['expensa_id',
                          'consorcio_id', 'consorcio_nombre',
                          'consorcio_cantidad_expensas', 'consorcio_prop_impagos',
                          'consorcio_prop_efectivo',
                          'unidad_id', 'unidad_denominacion', 'unidad_tipo', 
                          'unidad_prop_impagos',
                          'expensa_proporcion', 'expensa_interes_primer_vencimiento',
                          'expensa_ineteres_segundo_vencimiento', 'expensa_mes', 
                          'expensa_fecha', 'expensa_mes_pago', 'pago_metodo','target',
                          'expensa_mes_pago_anterior']]
    
    expensas.expensa_proporcion = expensas.expensa_proporcion / 100
    
    
    # Adicion de Informacion de 6 pagos anteriores
    pagos_anteriores = expensas[["unidad_id","expensa_mes_pago","expensa_mes_pago_anterior","pago_metodo"]]
    pagos_anteriores = pagos_anteriores.set_index(["unidad_id","expensa_mes_pago"])
    
    expensas = pd.merge(expensas,
             pagos_anteriores,
             left_on = ["unidad_id","expensa_mes_pago_anterior"],
             right_index = True,
             suffixes = ("", "_lag_1"))
    
    for i in range(2,4):
        expensas = pd.merge(expensas,
             pagos_anteriores,
             left_on = ["unidad_id","expensa_mes_pago_anterior_lag_{}".format(i-1)],
             right_index = True,
             suffixes = ("", "_lag_"+str(i)))
        
    expensas = expensas[['expensa_id', 'consorcio_id', 'consorcio_nombre',
               'consorcio_cantidad_expensas', 'consorcio_prop_impagos',
               'consorcio_prop_efectivo', 
               'unidad_id', 'unidad_denominacion',
               'unidad_tipo', 'unidad_prop_impagos', 
               'expensa_proporcion',
               'expensa_interes_primer_vencimiento',
               'expensa_ineteres_segundo_vencimiento', 'expensa_mes', 'expensa_fecha',
               'expensa_mes_pago','expensa_mes_pago_anterior',
               'pago_metodo_lag_1', 'pago_metodo_lag_2', 
               'pago_metodo_lag_3', 
               'target']]
    return expensas



data = create_dataset()
data.to_csv("../../data/processed/expensas_full_processed_lag_3.csv", index=False)