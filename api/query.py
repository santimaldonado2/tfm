import mysql.connector
from mysql.connector import Error
import pandas as pd
import requests
import json
import datetime
from dateutil.relativedelta import relativedelta
from sklearn.metrics import confusion_matrix, accuracy_score, f1_score, precision_score, recall_score



def consultar_db(consorcio, anio, mes):
    actual_month = datetime.datetime(anio, mes, 1)
    lag_6_month_date = actual_month + relativedelta(months=-6)
    df = None
    try:
        mySQLconnection = mysql.connector.connect(host='localhost',
                                                  database='eol',
                                                  user='root',
                                                  password='mysql')
        sql_select_Query = '''
        SELECT  e.id,
                u.id,
                e.mes,
                e.anio,
                ut.name,
                p.fecha,
                pm.nombre,
                e.fecha_vencimiento_1
        FROM expensa e
        LEFT JOIN pago p
        ON e.id = p.expensa_id
        LEFT JOIN pago_metodo pm
        ON p.metodo = pm.id
        JOIN unidades u
        ON e.unidad_id = u.id
        JOIN types_unidad ut
        ON u.type_id = ut.id
        WHERE e.deleted = 0
        AND   e.consorcio_id = {}
        AND   e.concepto_id IN (1,10, 599)
        AND   e.fecha_vencimiento_1 >= DATE("{}")
        AND   e.fecha_vencimiento_1 < DATE("{}");'''.format(consorcio,
                                                            lag_6_month_date.strftime("%Y-%m-%d"),
                                                            actual_month.strftime("%Y-%m-%d"))
        cursor = mySQLconnection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        df = pd.DataFrame.from_records(records,
                                       columns=["expensa_id", "unidad_id", "expensa_mes", "expensa_anio", "unidad_tipo",
                                                "pago_fecha", "pago_metodo", "expensa_primer_vencimiento"])

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        # closing database connection.
        if (mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")

    return df


def preparar_df(df, mes):
    df.expensa_primer_vencimiento = pd.to_datetime(df.expensa_primer_vencimiento, format="%Y-%m-%d")
    df.pago_fecha = pd.to_datetime(df.pago_fecha, format="%Y-%m-%d")
    df["expensa_mes_pago"] = pd.to_datetime(
        df.expensa_primer_vencimiento.dt.year.map(str) + '-' + df.expensa_primer_vencimiento.dt.month.map(str) + '-01',
        format="%Y-%m-%d")
    df.loc[df.pago_fecha >= (df.expensa_mes_pago + pd.DateOffset(months=1)), "pago_metodo"] = "Impago"

    df = df[["expensa_id", "unidad_id", "unidad_tipo", "pago_metodo", "expensa_mes_pago"]]

    pagos_dobles = df[["unidad_id", "expensa_mes_pago", "expensa_id"]].groupby(
        ["unidad_id", "expensa_mes_pago"]).count()
    pagos_dobles = pagos_dobles.loc[pagos_dobles.expensa_id > 1]

    pagos_dobles = pd.merge(pagos_dobles,
                            df[["unidad_id", "expensa_mes_pago", "expensa_id"]],
                            left_index=True,
                            right_on=["unidad_id", "expensa_mes_pago"])

    expensas_pagos_dobles = pagos_dobles.loc[::2, "expensa_id_y"]

    df = df.loc[~df.expensa_id.isin(expensas_pagos_dobles)]

    df.loc[df.pago_metodo.isna(), "pago_metodo"] = "Impago"

    unidades_tipos = \
        df[["unidad_id", "unidad_tipo", "expensa_id"]].groupby(["unidad_id", "unidad_tipo"]).count().reset_index()[
            ["unidad_id", "unidad_tipo"]]

    grouped = df.pivot("unidad_id", "expensa_mes_pago", "pago_metodo")

    grouped

    grouped = grouped.fillna("NS/NC")
    grouped = grouped.reset_index()

    grouped.columns = ["unidad_id", "pago_metodo_lag_6", "pago_metodo_lag_5", "pago_metodo_lag_4", "pago_metodo_lag_3",
                       "pago_metodo_lag_2", "pago_metodo_lag_1"]

    grouped["expensa_mes"] = '{:02d}'.format(mes)
    grouped = pd.merge(grouped,
                       unidades_tipos,
                       left_on="unidad_id",
                       right_on="unidad_id",
                       how="left")

    return grouped.to_dict(orient="records")


def consultar_api(consorcio, anio, mes):
    body = {}
    body["unidades"] = preparar_df(consultar_db(consorcio, anio, mes), mes)
    r = requests.post(url="http://127.0.0.1:5000/", json=body)
    return pd.DataFrame(json.loads(r.content))


def consultar_db_comparar(consorcio, anio, mes):
    actual_month = datetime.datetime(anio, mes, 1)
    end_date = actual_month + relativedelta(months=+1)
    df = None
    try:
        mySQLconnection = mysql.connector.connect(host='localhost',
                                                  database='eol',
                                                  user='root',
                                                  password='mysql')
        sql_select_Query = '''
        SELECT  e.id,
                u.id,
                e.mes,
                e.anio,
                ut.name,
                p.fecha,
                pm.nombre,
                e.fecha_vencimiento_1
        FROM expensa e
        LEFT JOIN pago p
        ON e.id = p.expensa_id
        LEFT JOIN pago_metodo pm
        ON p.metodo = pm.id
        JOIN unidades u
        ON e.unidad_id = u.id
        JOIN types_unidad ut
        ON u.type_id = ut.id
        WHERE e.deleted = 0
        AND   e.consorcio_id = {}
        AND   e.concepto_id IN (1,10, 599)
        AND   e.fecha_vencimiento_1 >= DATE("{}")
        AND   e.fecha_vencimiento_1 < DATE("{}");'''.format(consorcio,
                                                            actual_month.strftime("%Y-%m-%d"),
                                                            end_date.strftime("%Y-%m-%d"))
        cursor = mySQLconnection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        df = pd.DataFrame.from_records(records,
                                       columns=["expensa_id", "unidad_id", "expensa_mes", "expensa_anio", "unidad_tipo",
                                                "pago_fecha", "pago_metodo", "expensa_primer_vencimiento"])

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        # closing database connection.
        if (mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")

    return df


def preparar_df_comparar(df):
    df.expensa_primer_vencimiento = pd.to_datetime(df.expensa_primer_vencimiento, format="%Y-%m-%d")
    df.pago_fecha = pd.to_datetime(df.pago_fecha, format="%Y-%m-%d")
    df["expensa_mes_pago"] = pd.to_datetime(
        df.expensa_primer_vencimiento.dt.year.map(str) + '-' + df.expensa_primer_vencimiento.dt.month.map(str) + '-01',
        format="%Y-%m-%d")
    df.loc[df.pago_fecha >= (df.expensa_mes_pago + pd.DateOffset(months=1)), "pago_metodo"] = "Impago"

    df = df[["expensa_id", "unidad_id", "unidad_tipo", "pago_metodo", "expensa_mes_pago"]]
    pagos_dobles = df[["unidad_id", "expensa_mes_pago", "expensa_id"]].groupby(
        ["unidad_id", "expensa_mes_pago"]).count()
    pagos_dobles = pagos_dobles.loc[pagos_dobles.expensa_id > 1]

    pagos_dobles = pd.merge(pagos_dobles,
                            df[["unidad_id", "expensa_mes_pago", "expensa_id"]],
                            left_index=True,
                            right_on=["unidad_id", "expensa_mes_pago"])

    expensas_pagos_dobles = pagos_dobles.loc[::2, "expensa_id_y"]

    df = df.loc[~df.expensa_id.isin(expensas_pagos_dobles)]

    df.loc[df.pago_metodo.isna(), "pago_metodo"] = "Impago"

    grouped = df.pivot("unidad_id", "expensa_mes_pago", "pago_metodo")

    grouped = grouped.reset_index()

    grouped.columns = ["unidad_id", "target"]

    grouped.loc[grouped.target != "Impago", "target"] = False
    grouped.loc[grouped.target == "Impago", "target"] = True

    return grouped


df_response = consultar_api(1618, 2019, 2)
df_target = preparar_df_comparar(consultar_db_comparar(1618, 2019, 2))

df_final = pd.merge(
    df_response.loc[:, ["unidad_id",
                        "unidad_tipo",
                        "expensa_mes",
                        "pago_metodo_lag_6",
                        "pago_metodo_lag_5",
                        "pago_metodo_lag_4",
                        "pago_metodo_lag_3",
                        "pago_metodo_lag_2",
                        "pago_metodo_lag_1",
                        "prediccion_naive",
                        "prediccion_lda",
                        "prediccion_xgb",
                        "prediccion_lstm"]],
    df_target,
    left_on="unidad_id",
    right_on="unidad_id"
)



df_metricas = []
for model_column in ["prediccion_naive", "prediccion_lda", "prediccion_xgb", "prediccion_lstm"]:
    prediccion_list = []
    prediccion_list.append(model_column),
    prediccion_list.append(accuracy_score(df_final.target, df_final[model_column]))
    prediccion_list.append(f1_score(df_final.target, df_final[model_column]))
    prediccion_list.append(precision_score(df_final.target, df_final[model_column]))
    prediccion_list.append(recall_score(df_final.target, df_final[model_column]))
    df_metricas.append(prediccion_list)

df_metricas = pd.DataFrame(df_metricas, columns=["modelo", "accuracy", "f1","precision", "recall"])


