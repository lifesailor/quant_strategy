import tensorflow as tf
import numpy as np


class CRNN_model:
    def __init__(self, params):
        self.num_classes = params['num_classes']

        self.use_cnn = params['use_cnn']
        self.num_filters = params['num_filters']
        self.filter_sizes = params['filter_size']
        self.cnn_batch_norm = params['cnn_batch_norm']
        self.pool_sizes = params['pool_sizes']
        self.cnn_dropout_keep_prob = params['cnn_dropout_keep_prob']

        self.use_fc = params['use_fc']
        self.fc_hidden_units = params['fc_hidden_units']
        self.fc_batch_norm = params['fc_batch_norm']
        self.fc_dropout_keep_prob = params['fc_dropout_keep_prob']

        self.use_rnn = params['use_rnn']
        self.rnn_n_hiddens = params['rnn_n_hiddens']
        self.rnn_dropout_keep_prob = params['rnn_dropout_keep_prob']
        self.use_layer_normalization = params['use_layer_normalization']

        self.bi_rnn = params['bi_rnn']
        self.use_gap = params['use_gap']

        if params['activation'] == 'relu':
            self.activation = tf.nn.relu
        elif params['activation'] == 'sigmoid':
            self.activation = tf.nn.sigmoid
        else:
            self.activation = params['activation']
        self.idx_convolutional_layers = range(1, len(self.filter_sizes) + 1)
        self.idx_fc_layers = range(1, len(self.fc_hidden_units) + 1)
        self.idx_rnn_layers = range(1, len(self.rnn_n_hiddens) + 1)

    def convolutional_layers(self, X, is_training=True, reuse=False):
        inputs = X

        for i, num_filter, filter_size, use_bn, pool_size, keep_prob in zip(self.idx_convolutional_layers,
                                                                            self.num_filters,
                                                                            self.filter_sizes,
                                                                            self.cnn_batch_norm,
                                                                            self.pool_sizes,
                                                                            self.cnn_dropout_keep_prob):
            L = tf.layers.conv2d(inputs,
                                 filters=num_filter,
                                 kernel_size=filter_size,
                                 strides=1,
                                 padding='SAME',
                                 name='CONV' + str(i),
                                 reuse=reuse)
            if use_bn:
                L = tf.layers.batch_normalization(L, training=is_training, name='BN' + str(i), reuse=reuse)
            L = self.activation(L)

            if keep_prob:
                L = tf.layers.dropout(L, keep_prob, training=is_training)

            if pool_size != 1:
                L = tf.layers.max_pooling2d(L, pool_size=pool_size, strides=pool_size, padding='SAME')

            inputs = L
        return inputs

    def fc_layers(self, X, is_training=True, reuse=False):
        inputs = X

        for i, units, use_bn, keep_prob in zip(self.idx_fc_layers,
                                               self.fc_hidden_units,
                                               self.fc_batch_norm,
                                               self.fc_dropout_keep_prob):
            fc = tf.layers.dense(inputs,
                                 units=units,
                                 reuse=reuse,
                                 name='FC' + str(i))

            if use_bn:
                fc = tf.layers.batch_normalization(fc, training=is_training, name='fc_BN' + str(i), reuse=reuse)
            fc = self.activation(fc)

            if keep_prob:
                fc = tf.layers.dropout(fc, rate=keep_prob, training=is_training, name='fc_dropout' + str(i))
            inputs = fc
        return inputs

    def bi_rnn_layers(self, inputs, is_training=True, reuse=False):
        if is_training:
            keep_probs = self.rnn_dropout_keep_prob

        else:
            keep_probs = np.ones_like(self.rnn_dropout_keep_prob)

        for i, n_hidden, keep_prob in zip(self.idx_rnn_layers, self.rnn_n_hiddens, keep_probs):
            if self.use_layer_normalization:
                cell_fw = tf.contrib.rnn.LayerNormBasicLSTMCell(self.rnn_n_hiddens[0],
                                                                reuse=reuse,
                                                                dropout_keep_prob=keep_probs[0])
                cell_bw = tf.contrib.rnn.LayerNormBasicLSTMCell(self.rnn_n_hiddens[0],
                                                                reuse=reuse,
                                                                dropout_keep_prob=keep_probs[0])
            else:
                cell_fw = tf.nn.rnn_cell.BasicLSTMCell(self.rnn_n_hiddens[0], reuse=reuse)
                cell_fw = tf.nn.rnn_cell.DropoutWrapper(cell_fw, output_keep_prob=keep_probs[0])
                cell_bw = tf.nn.rnn_cell.BasicLSTMCell(self.rnn_n_hiddens[0], reuse=reuse)
                cell_bw = tf.nn.rnn_cell.DropoutWrapper(cell_bw, output_keep_prob=keep_probs[0])

            (outputs_fw, outputs_bw), (states_fw, states_bw) = tf.nn.bidirectional_dynamic_rnn(cell_fw,
                                                                                               cell_bw,
                                                                                               inputs,
                                                                                               dtype=tf.float32,
                                                                                               scope='BLSTM' + str(i))

            inputs = tf.concat([outputs_fw, outputs_bw], 2)
        outputs = tf.transpose(inputs, [1, 0, 2])
        outputs = outputs[-1]
        return outputs

    def rnn_layers(self, inputs, is_training=True, reuse=False):
        if is_training:
            keep_probs = self.rnn_dropout_keep_prob
        else:
            keep_probs = np.ones_like(self.rnn_dropout_keep_prob)

        # single layer
        if len(self.idx_rnn_layers) == 1:
            if self.use_layer_normalization:
                cell = tf.contrib.rnn.LayerNormBasicLSTMCell(self.rnn_n_hiddens[0], reuse=reuse,
                                                             dropout_keep_prob=keep_probs[0])
            else:
                cell = tf.nn.rnn_cell.BasicLSTMCell(self.rnn_n_hiddens[0], reuse=reuse)
                cell = tf.nn.rnn_cell.DropoutWrapper(cell, output_keep_prob=keep_probs[0])
        # multi layer
        else:
            cell_list = []

            for i, n_hidden, keep_prob in zip(self.idx_rnn_layers, self.rnn_n_hiddens, keep_probs):
                if self.use_layer_normalization:
                    cell_ = tf.contrib.rnn.LayerNormBasicLSTMCell(n_hidden, reuse=reuse, dropout_keep_prob=keep_prob)
                else:
                    cell_ = tf.nn.rnn_cell.BasicLSTMCell(n_hidden, reuse=reuse)
                    cell_ = tf.nn.rnn_cell.DropoutWrapper(cell_, output_keep_prob=keep_prob)

                cell_list.append(cell_)

            cell = tf.nn.rnn_cell.MultiRNNCell(cell_list)

        # output_shape [batch_size, width(n_step), n_classes]
        outputs, states = tf.nn.dynamic_rnn(cell, inputs, dtype=tf.float32)
        outputs = tf.transpose(outputs, [1, 0, 2])
        outputs = outputs[-1]
        return outputs

    def get_reshaped_cnn_to_rnn(self, inputs):
        # [batch, height, width, n_feature map]
        shape = inputs.get_shape().as_list()

        # 우리가 얻어야하는 사이즈 [batch, height, width x n_feature map]
        # [batch, n_steps, vector]
        reshaped_inputs = tf.reshape(inputs, [-1, shape[1], shape[2] * shape[3]])

        return reshaped_inputs

    def __call__(self, X, is_training=True, reuse=False):

        L = X

        if self.use_cnn:
            L = self.convolutional_layers(L, is_training, reuse)

        if self.use_rnn:

            if self.use_cnn:
                L = self.get_reshaped_cnn_to_rnn(L)
            if self.bi_rnn:
                L = self.bi_rnn_layers(L, is_training, reuse)
            else:
                L = self.rnn_layers(L, is_training, reuse)

        if self.use_gap:
            shape = L.get_shape().as_list()

            # 글로벌 풀링 사이즈 (height, width)
            pool_size = (shape[1], shape[2])
            L = tf.layers.average_pooling2d(L, pool_size=pool_size, strides=1, padding='VALID')

            # 마지막 dense layer를 위한 flatten
            L = tf.layers.flatten(L)

        if self.use_fc:
            if not self.use_gap:
                L = tf.layers.flatten(L)
            L = self.fc_layers(L, is_training, reuse)
        return L
