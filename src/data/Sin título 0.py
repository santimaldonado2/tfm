# -*- coding: utf-8 -*-
"""
Created on Mon May 20 10:10:29 2019

@author: smaldonado
"""
import pandas as pd
expensas_raw = pd.read_csv("../../data/raw/expensas_full2.csv",
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
                            "expensa_concepto": "object"
                        })

expensas_raw.dtypes



expensas_raw.isna().sum()

expensas_raw.loc[expensas_raw.expensa_id == 438199,:]