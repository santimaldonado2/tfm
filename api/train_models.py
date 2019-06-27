import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from keras.layers.core import Dense, Dropout, Activation, Flatten, Reshape
from keras.models import Sequential, Model
from keras.layers import LSTM, Embedding, Concatenate, Input

data_lag_n = pd.read_csv("data/processed/pagos_por_unidad.csv",
                         dtype={
                             "unidad_id": "object",
                         },
                         parse_dates=["expensa_mes_pago"])

data = pd.read_csv("data/processed/expensas_full_processed_vis.csv",
                   dtype={
                       "expensa_id": "object",
                       "unidad_id": "object",
                       "consorcio_id": "object",
                       "expensa_mes": "object",
                   },
                   parse_dates=["expensa_fecha", "expensa_mes_pago", "expensa_mes_pago_anterior"])

final_data = pd.merge(
    data[["expensa_id", "unidad_id", "unidad_tipo", "expensa_mes", "expensa_mes_pago"]],
    data_lag_n,
    left_on=["unidad_id", "expensa_mes_pago"],
    right_on=["unidad_id", "expensa_mes_pago"],
    how="inner"
)

model_columns_6 = ['unidad_tipo', 'expensa_mes', 'pago_metodo_lag_6', 'pago_metodo_lag_5',
                   'pago_metodo_lag_4', 'pago_metodo_lag_3', 'pago_metodo_lag_2',
                   'pago_metodo_lag_1']
model_columns = ['unidad_tipo', 'expensa_mes', 'pago_metodo_lag_3', 'pago_metodo_lag_2',
                 'pago_metodo_lag_1']

final_data = final_data.sort_values("expensa_mes_pago").reset_index().drop(columns="index")
min_cv_id = final_data.loc[final_data.expensa_mes_pago == "2018-06-01", "expensa_mes_pago"].idxmin()
min_test_id = final_data.loc[final_data.expensa_mes_pago == "2018-09-01", "expensa_mes_pago"].idxmin()


def get_train_test_split(X_columns):
    X_train = pd.get_dummies(final_data.loc[0:min_test_id, X_columns], drop_first=True)
    X_test = pd.get_dummies(final_data.loc[min_test_id:, X_columns], drop_first=True)
    Y_train = final_data.loc[0:min_test_id, "target"]
    Y_test = final_data.loc[min_test_id:, "target"]

    drop_test_columns = [column for column in X_test.columns if column not in X_train.columns]
    X_test = X_test.drop(columns=drop_test_columns)

    for column in [column for column in X_train.columns if column not in X_test.columns]:
        X_test[column] = 0

    X_test = X_test[X_train.columns]
    print(X_train.shape, Y_train.shape)
    print(X_test.shape, Y_test.shape)
    return X_train, Y_train, X_test, Y_test


X_train, Y_train, X_test, Y_test = get_train_test_split(model_columns)


def get_train_test_meassures(model, X_train, Y_train, X_test, Y_test):
    train_preds = model.predict(X_train)
    print("accuracy:", accuracy_score(Y_train, train_preds))
    print("precision:", precision_score(Y_train, train_preds))
    print("recall:", recall_score(Y_train, train_preds))
    print("f1:", f1_score(Y_train, train_preds))
    print("--------------------------------------------------")
    test_preds = model.predict(X_test)
    print("accuracy:", accuracy_score(Y_test, test_preds))
    print("precision:", precision_score(Y_test, test_preds))
    print("recall:", recall_score(Y_test, test_preds))
    print("f1:", f1_score(Y_test, test_preds))


X_train, Y_train, X_test, Y_test = get_train_test_split(
    ['unidad_tipo', 'expensa_mes', 'pago_metodo_lag_3', 'pago_metodo_lag_2',
     'pago_metodo_lag_1'])

logReg = LogisticRegression(
    C=1000000,
    solver="lbfgs",
    penalty="l2",
    max_iter=200,
)
logReg.fit(X_train, Y_train)
get_train_test_meassures(logReg, X_train, Y_train, X_test, Y_test)

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.model_selection import GridSearchCV

param_grid = {
    "solver": ["svd", "lsqr", "eigen"],
    "shrinkage": [None, "auto"]
}

lda = GridSearchCV(
    estimator=LinearDiscriminantAnalysis(),
    param_grid=param_grid,
    scoring='f1',
    n_jobs=-1,
    cv=[(list(range(0, min_cv_id)), list(range(min_cv_id, min_test_id)))],
    return_train_score=True,
    error_score=0,
    verbose=5
)

lda.fit(X_train.values, Y_train.values)

lda_model = lda.best_estimator_

import pickle

with open("models/api/lda.pkl", "wb") as file:
    pickle.dump(lda_model, file)

xgb = XGBClassifier(
    learning_rate=0.02199388848943524,
    max_depth=2,
    n_estimators=1987,
    reg_alpha=0.9557797519273585)
xgb.fit(X_train, Y_train)

with open("models/api/xgb.pkl", "wb") as file:
    pickle.dump(xgb, file)

from keras import backend as K


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


def get_train_test_split_2(X_columns):
    pagos_columns = [column for column in X_columns if column[0:11] == "pago_metodo"]
    estructural_columns = [column for column in X_columns if column[0:11] != "pago_metodo"]

    X_train_estructural = pd.get_dummies(final_data.loc[0:min_test_id, estructural_columns], drop_first=True)
    X_train_pagos = final_data.loc[0:min_test_id, pagos_columns]

    X_test_estructural = pd.get_dummies(final_data.loc[min_test_id:, estructural_columns], drop_first=True)
    X_test_pagos = final_data.loc[min_test_id:, pagos_columns]

    X_test_pagos = transform_values(X_test_pagos)
    X_train_pagos = transform_values(X_train_pagos)

    Y_train = final_data.loc[0:min_test_id, "target"]
    Y_test = final_data.loc[min_test_id:, "target"]

    for column in [column for column in X_train.columns if column not in X_test_estructural.columns]:
        X_test_estructural[column] = 0

    X_test_estructural = X_test_estructural[X_train_estructural.columns]
    print(X_train_estructural.shape, X_train_pagos.shape, Y_train.shape)
    print(X_test_estructural.shape, X_test_pagos.shape, Y_test.shape)
    return X_train_estructural, X_train_pagos, Y_train, X_test_estructural, X_test_pagos, Y_test


X_train_estructural, X_train_pagos, Y_train, X_test_estructural, X_test_pagos, Y_test = get_train_test_split_2(
    model_columns_6)


def build_lstm_model(embeds_size=6, dense_size=20, dropout_rate=0.1):
    estructural_input = Input(shape=(18,), name="estructural_input")
    pagos_input = Input(shape=(6,), name="pagos_input")

    embeds = Embedding(input_dim=6, output_dim=embeds_size, input_length=6)(pagos_input)
    lstm = LSTM(10, activation="relu")(embeds)

    concat = Concatenate()([estructural_input, lstm])
    dense = Dense(dense_size, activation="relu")(concat)
    dropout = Dropout(dropout_rate)(dense)
    output = Dense(1, activation="sigmoid", name="output")(dropout)

    model = Model(inputs=[estructural_input, pagos_input], outputs=[output])
    model.compile(optimizer='adam', loss={'output': 'binary_crossentropy'}, metrics=["acc", f1])
    return model


params = {
    "embeds_size": 6,
    "dense_size": 30,
    "dropout_rate": 0.3,
    "epochs": 5
}

lstm = build_lstm_model(embeds_size=params["embeds_size"],
                        dense_size=params["dense_size"],
                        dropout_rate=params["dropout_rate"])
lstm.fit({
    "estructural_input": X_train_estructural.values,
    "pagos_input": X_train_pagos.values
},
    Y_train,
    epochs=params["epochs"],
    batch_size=1000)

lstm.save("models/api/lstm.pkl")