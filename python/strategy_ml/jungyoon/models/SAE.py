import tensorflow as tf
from nn_utils import get_activation_for_layers, rmse_loss


class Autoencoder:
    '''
    simple autoencoder with 1 hidden layer
    '''

    def __init__(self, before_unit, unit, activation, optimizer, loss_fn, bn=False):
        self.optimizer = optimizer
        self.loss_fn = loss_fn
        self.encoder = tf.keras.layers.Dense(unit, activation=activation)

        self.decoder = tf.keras.layers.Dense(before_unit)

        self.model = tf.keras.Sequential()
        self.model.add(self.encoder)
        if bn:
            self.bn = tf.keras.layers.BatchNormalization()
            self.model.add(self.bn)
        self.model.add(self.decoder)
        self.model.compile(self.optimizer, self.loss_fn)

    def encode(self, inputs, training=True):
        outputs = self.encoder(inputs)
        #         if self.bn:
        #             outputs = self.bn(outputs, training = training)
        return outputs

    def decode(self, inputs):
        return self.decoder(inputs)

    def fit(self, data, batch_size, epochs, verbose=True):
        return self.model.fit(data, data, batch_size=batch_size, epochs=epochs, verbose=verbose)

    def call(self, inputs, training=True, reduction=True):
        with tf.name_scope('encoder'):
            self.encoded = self.encode(inputs, training=training)
        if not training and reduction:
            return self.encoded
        with tf.name_scope('decoder'):
            self.decoded = self.decode(self.encoded)
        return self.decoded


class SAE:
    """
    make greedy trained Autoencoder
    pretrain sub autoencoder for every layers

    parameter
    ---------------------
    n_features: number of features
    units: number of units, length of list would be number of layers (default : [128, 64, 32])
    batch_norm : list of usage of batchnormalization True or False. The length of list should be same as that of units list (default : [False, False, False])
    activation: activation function for NN (default : 64)
    learning_rate: training learning rate (default : 64)
    batch_size : training batch_size (default : 64)
    loss_fn : loss function to train, MSE, MAE, RMAE is available (default : MSE)
    epochs : training epochs (default : 120)
    name: model_name (default: None)

    """

    def __init__(self, params, name=None):
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
        #         self.batch_norm= batch_norm

        self.auto_encoders = []

        before_unit = self.n_features
        for i in range(len(self.units)):
            opt = tf.keras.optimizers.Adam(self.learning_rate)
            autoencoder = Autoencoder(before_unit, self.units[i], activation=self.activation, optimizer=opt,
                                      loss_fn=self.loss_fn, bn=self.batch_norm[i])
            self.auto_encoders.append(autoencoder)
            before_unit = self.units[i]

    def get_encoder_objects(self):
        encoders = []
        for auto_encoder in self.auto_encoders:
            encoders.append(auto_encoder.encoder)
            if hasattr(auto_encoder, 'bn'):
                encoders.append(auto_encoder.bn)
        return tf.keras.Sequential(encoders, name='SAE')

    def fit(self, data, verbose=True):
        #         self.trainable_weights = []
        for i, model in enumerate(self.auto_encoders):
            print('{}th autoencoder training'.format(i + 1))
            model.fit(data, batch_size=self.batch_size, epochs=self.epochs, verbose=verbose)
            #             print(model(data))
            data = model.encode(data).numpy()

    #             self.trainable_weights += model.trainable_weights

    def call(self, inputs, reduction=True):
        outputs = inputs
        for auto_encoder in self.auto_encoders:
            outputs = auto_encoder.encode(outputs)
        # 차원축소 안한다면 decoder 사용해서 reconstruct
        # 차원축소 한다면 sub_model들의 decoder들은 쓰이지 않음
        if not reduction:
            for auto_encoder in self.auto_encoders[::-1][:-1]:
                outputs = auto_encoder.decode(outputs)
                outputs = self.activation(outputs)
            outputs = self.auto_encoders[0].decode(outputs)
        return outputs


if __name__ == '__main__':
    SAE_params = {
        'batch_size': 64,
        'learning_rate': 0.001,
        'units': [128, 64, 32],
        'batch_norm': [False, False, False],
        'activation': tf.nn.sigmoid,
        'epochs': 100,
        'n_features': 102,
        'loss_function': 'MSE',
    }
    sae = SAE(SAE_params)
    sae.pretrain(tf.random_normal([1000, SAE_params['n_features']]))
