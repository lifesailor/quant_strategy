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


def batch_iter(*data, batch_size, shuffle=True, end=True):
    """

    :param data: dataset, can be several dataset ex) batch_iter(data, target, batch_size = 32))
    :param batch_size: batch size to yield
    :param shuffle: shuffling whole data
    :param end: if False, abandon last data which less than batch_size (default : True)
    """
    if shuffle:
        shuffle_mask = np.random.permutation(len(data[0]))
    batch = []
    for d in data:
        if shuffle:
            d = d[shuffle_mask]
        batch.append(d)
    idx = 0
    length = len(data[0])
    if not end:
        length -= batch_size
    while idx < length:
        if len(batch) == 1:
            yield batch[0][idx: idx + batch_size]
        else:
            yield tuple(map(lambda x: x[idx: idx + batch_size], batch))
        idx += batch_size
    return



def class_iter(data, target, sample_numbers=[20, 30, 20, 30], num_classes = None):
    """

    :param data: data to use
    :param target: target data can be onehot data
    :param sample_numbers: number of samples which batch would be include, Its length should same with num_classes
    :return: batch_data, batch_target(one-hot encoded)
    """

    # 인코딩된 y면 다시 int형태로 바꿔준다.
    if len(target.shape) != 1:
        target_ = np.argmax(target, 1)
    else:
        target_ = target
    if num_classes is None:
        num_classes = max(target_) + 1

    try:
        assert num_classes == len(sample_numbers)
    except AssertionError:
        raise AssertionError(
            'target num_classes do not match with sample number it should be same (n_classes = max(target)+1 = {}, len(sample_numbers) = {}'.format(
                num_classes, len(sample_numbers)))

    class_generators = []
    cls_data = []
    for i in range(len(sample_numbers)):
        cls_idx = target_ == i
        cls_data.append((data[cls_idx], target_[cls_idx]))
        # 각자의 클래스에 따라 따로 generator 생성
        x_, y_ = cls_data[i]
        class_generators.append(batch_iter(x_, y_, batch_size=sample_numbers[i], end = False))
    # 계속 반복
    while True:
        batch_data = []
        batch_target = []
        for i in range(len(class_generators)):
            try:
                x, y = next(class_generators[i])
            except StopIteration:
                x_, y_ = cls_data[i]
                class_generators[i] = batch_iter(x_, y_, batch_size=sample_numbers[i], end = False)
                x, y = next(class_generators[i])
            batch_data.append(x)
            batch_target.append(y)
        batch_data = np.concatenate(batch_data)
        batch_target = np.eye(len(sample_numbers))[np.concatenate(batch_target)]

        shuffle = np.random.permutation(len(batch_data))
        batch_data = batch_data[shuffle]
        batch_target = batch_target[shuffle]

        yield batch_data, batch_target