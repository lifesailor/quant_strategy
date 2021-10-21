import tensorflow as tf
import numpy as np


def get_activation_for_layers(activation_name):
    activation_name_ = activation_name.lower()
    if activation_name_ == 'relu':
        return tf.keras.layers.ReLU
    elif activation_name_ == 'prelu':
        return tf.keras.layers.PReLU
    elif activation_name_ == 'leakyrelu' or activation_name_ == 'leaky_relu':
        return tf.keras.layers.LeakyReLU
    elif activation_name_ == 'elu':
        return tf.keras.layers.ELU


def rmse_loss(y_true, y_pred):
    eps = 1e-5
    return tf.reduce_mean(tf.math.sqrt(tf.abs(y_true - y_pred) + eps))


def nll_loss(y_true, y_pred, var):
    mean = y_pred
    sigma = tf.log(1. + tf.exp(var))
    return 0.5 * tf.reduce_mean(tf.log(sigma)) + 0.5 * tf.reduce_mean(
        tf.div(tf.square(y_true - mean), sigma)) + 0.5 * tf.log(2 * np.pi)


def batch_iter(*data, batch_size, shuffle=True):
    if shuffle:
        shuffle_mask = np.random.permutation(len(data[0]))
    batch = []
    for d in data:
        if shuffle:
            d = d[shuffle_mask]
        batch.append(d)
    idx = 0
    length = len(data[0])
    while idx < length:
        if len(batch) == 1:
            yield batch[0][idx: idx + batch_size]
        else:
            yield tuple(map(lambda x: x[idx: idx + batch_size], batch))
        idx += batch_size