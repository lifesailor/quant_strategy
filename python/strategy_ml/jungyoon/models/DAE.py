
import tensorflow as tf
from nn_utils import get_activation_for_layers, rmse_loss, batch_iter
import numpy as np

class DAE:
    def __init__(self, params, name=None):
        if name:
            self.name = name
        else:
            self.name = 'DAE'
        self.activation = params['activation']
        self.learning_rate = params['learning_rate']
        self.units = params['units']
        self.n_features = params['n_features']
        self.batch_size = params['batch_size']
        self.epochs = params['epochs']
        self.batch_norm = params['batch_norm']
        if params['loss_function'] == 'MSE':
            self.loss_fn = tf.losses.mean_squared_error
        elif params['loss_function'] == 'MAE':
            self.loss_fn = tf.losses.absolute_difference
        elif params['loss_function'] == 'RMSE':
            self.loss_fn = rmse_loss
        self.noise_ratio = params['noise_ratio']

        self.encoders = []

        for i in range(len(self.units)):
            self.encoders.append(tf.keras.layers.Dense(self.units[i], activation=self.activation))
            if self.batch_norm[i]:
                self.encoders.append(tf.keras.layers.Batch_normalization())

        self.auto_encoders = self.encoders
        for i in range(len(self.units) - 1, -1, -1):
            self.auto_encoders.append(tf.keras.layers.Dense(self.units[i], activation=self.activation))
            if self.batch_norm[i]:
                self.auto_encoders.append(tf.keras.layers.Batch_normalization())
        self.auto_encoders.append(tf.keras.layers.Dense(self.n_features))

        self.model = tf.keras.Sequential(self.auto_encoders, name='DAE')
        self.optimizer = tf.keras.optimizers.Adam(self.learning_rate)

        self.model.compile(self.optimizer, self.loss_fn)

    def get_encoder_objects(self):
        return tf.keras.Sequential(self.auto_encoders, name='DAE')

    def fit(self, data, verbose=False):
        for epoch in range(self.epochs):
            total_batch = int(len(data) / self.batch_size) + 1
            total_c = 0
            for batch_x in batch_iter(data, batch_size=self.batch_size, shuffle=True):
                noised_batch_x = self.noise_injection(batch_x, data, self.noise_ratio)
                c = self.model.train_on_batch(noised_batch_x, batch_x)
                total_c += c
            if verbose:
                print('Epoch : {}, cost : {}'.format(epoch, total_c / total_batch))
        # noised_data = self.noise_injection(data)
        # self.model.fit(data, batch_size = self.batch_size, epochs = self.epochs, verbose = verbose)

    def noise_injection(self, batch, all_data, noise_ratio=0.07):
        batch_data = batch.copy()
        batch_size, n_features = batch_data.shape
        entire_length = len(all_data)
       
        random_row = np.random.randint(0, entire_length, size=batch_size)
        for i in range(batch_size):
            random_swap = np.random.sample(n_features) < self.noise_ratio
            batch_data[i, random_swap] = all_data[random_row[i], random_swap]
        return batch_data

    def get_reconstructed(self, inputs, latent=False):
        re_encoder, re_decoder = self.get_logits(inputs, True, False)
        if latent:
            return re_encoder
        return re_decoder