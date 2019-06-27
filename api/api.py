from flask import Flask, request
from flask_restful import Resource, Api
import json
import pickle
import pandas as pd
from keras.models import load_model
from keras import backend as K
import tensorflow as tf

app = Flask(__name__)
api = Api(app)

prediction_columns = ['unidad_tipo_Casa', 'unidad_tipo_Cochera', 'unidad_tipo_Departamento',
                      'unidad_tipo_Duplex', 'unidad_tipo_Local', 'unidad_tipo_Lote',
                      'unidad_tipo_Oficina', 'expensa_mes_02', 'expensa_mes_03',
                      'expensa_mes_04', 'expensa_mes_05', 'expensa_mes_06', 'expensa_mes_07',
                      'expensa_mes_08', 'expensa_mes_09', 'expensa_mes_10', 'expensa_mes_11',
                      'expensa_mes_12', 'pago_metodo_lag_3_EntePago',
                      'pago_metodo_lag_3_Impago', 'pago_metodo_lag_3_Internet',
                      'pago_metodo_lag_3_NS/NC', 'pago_metodo_lag_2_EntePago',
                      'pago_metodo_lag_2_Impago', 'pago_metodo_lag_2_Internet',
                      'pago_metodo_lag_2_NS/NC', 'pago_metodo_lag_1_EntePago',
                      'pago_metodo_lag_1_Impago', 'pago_metodo_lag_1_Internet',
                      'pago_metodo_lag_1_NS/NC', 'pago_metodo_lag_1_Otro']

lstm_estructural_columns = ['unidad_tipo_Casa', 'unidad_tipo_Cochera', 'unidad_tipo_Departamento',
                            'unidad_tipo_Duplex', 'unidad_tipo_Local', 'unidad_tipo_Lote',
                            'unidad_tipo_Oficina', 'expensa_mes_02', 'expensa_mes_03',
                            'expensa_mes_04', 'expensa_mes_05', 'expensa_mes_06', 'expensa_mes_07',
                            'expensa_mes_08', 'expensa_mes_09', 'expensa_mes_10', 'expensa_mes_11',
                            'expensa_mes_12']

with open("./../models/api/lda.pkl", "rb") as file:
    lda = pickle.load(file)
with open("./../models/api/xgb.pkl", "rb") as file:
    xgb = pickle.load(file)

graph = tf.get_default_graph()

def f1(y_true, y_pred):
    def recall(y_true, y_pred):
        """Recall metric.

        Only computes a batch-wise average of recall.

        Computes the recall, a metric for multi-label classification of
        how many relevant items are selected.
        """
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
        recall = true_positives / (possible_positives + K.epsilon())
        return recall

    def precision(y_true, y_pred):
        """Precision metric.

        Only computes a batch-wise average of precision.

        Computes the precision, a metric for multi-label classification of
        how many selected items are relevant.
        """
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
        precision = true_positives / (predicted_positives + K.epsilon())
        return precision

    precision = precision(y_true, y_pred)
    recall = recall(y_true, y_pred)
    return 2 * ((precision * recall) / (precision + recall + K.epsilon()))


lstm = load_model("./../models/api/lstm.pkl", custom_objects={"f1": f1})
lstm_threshold = 0.38


def getDataFrame(unidades_list):
    return pd.DataFrame(unidades_list)


def change_metodo_pago(metodo_pago):
    if not metodo_pago or metodo_pago == "Impago":
        return "Impago"
    if metodo_pago in ["Efectivo", "Cheque"]:
        return "Efec-Cheque"
    if metodo_pago in ["Rapipago", "Pago Facil"]:
        return "EntePago"
    if metodo_pago in ["Transferencia", "Pago Mis Cuentas", "Link Pagos", "Bapropagos", "Débito directo",
                       "Débito Santander"]:
        return "Internet"
    return "Otro"


def prepareToPredict(df):
    pagos_columns = [column for column in df.columns if column[:11] == "pago_metodo"]
    for column in pagos_columns:
        df[column] = df[column].map(change_metodo_pago)
    df = pd.get_dummies(df)
    for column in [column for column in prediction_columns if column not in df.columns]:
        df[column] = 0

    return df[prediction_columns]


words_dict = {
    "Impago": 0,
    "Internet": 1,
    "EntePago": 2,
    "Efec-Cheque": 3,
    "Otro": 4,
    "NS/NC": 5
}


def transform_values(X_i):
    for column in X_i.columns:
        X_i[column] = X_i[column].map(lambda x: words_dict[x])
    return X_i


def prepareToPredictLstm(df):
    pagos_columns = [column for column in df.columns if column[:11] == "pago_metodo"]
    df_estructural = pd.get_dummies(df[[column for column in df.columns if column not in pagos_columns]])
    df_pagos = df[pagos_columns]
    for column in [column for column in lstm_estructural_columns if column not in df_estructural.columns]:
        df_estructural[column] = 0

    df_pagos = transform_values(df_pagos)
    df_estructural = df_estructural[lstm_estructural_columns]
    return df_estructural, df_pagos


def predict_lstm(df):
    df_estructural, df_pagos = prepareToPredictLstm(df)
    with graph.as_default():
        probas = lstm.predict({
            "estructural_input": df_estructural,
            "pagos_input": df_pagos
        })
    return probas > lstm_threshold


class Predict(Resource):
    def get(self):
        return "Hello World"

    def post(self):
        data = request.data
        dataDict = json.loads(data)
        unidades_df = getDataFrame(dataDict["unidades"])
        print(unidades_df.loc[:10,:])
        unidades_predict_df = prepareToPredict(unidades_df)
        print(unidades_predict_df.loc[:10,:])
        unidades_df["prediccion_naive"] = unidades_df["pago_metodo_lag_1"] == "Impago"
        unidades_df["prediccion_lda"] = lda.predict(unidades_predict_df)
        unidades_df["prediccion_xgb"] = xgb.predict(unidades_predict_df)
        unidades_df["prediccion_lstm"] = predict_lstm(unidades_df)

        return unidades_df.to_dict(orient='records')


api.add_resource(Predict, '/')

if __name__ == '__main__':
    app.run(debug=True)
