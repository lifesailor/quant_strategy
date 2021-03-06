import pickle
import tensorflow as tf
from nn_utils import get_activation_for_layers, rmse_loss


class Residual_DNN_block(tf.keras.layers.Layer):
    def __init__(self,
                 ffn_dim,
                 activation,
                 keep_prob,
                 kernel_initializer='glorot_uniform',
                 kernel_regularizer=None,
                 *args, **kwargs):
        super(Residual_DNN_block, self).__init__(*args, **kwargs)
        self.ffn_dim = ffn_dim
        self.keep_prob = keep_prob
        self.activation_name = activation
        self.activation = get_activation_for_layers(activation)
        self.regularizer = kernel_regularizer
        self.kernel_initializer = kernel_initializer

    def build(self, input_shape):
        self.first_layer = tf.keras.layers.Dense(self.ffn_dim,
                                                 kernel_initializer=self.kernel_initializer,
                                                 kernel_regularizer=self.regularizer)
        self.activation1 = self.activation()
        self.dropout_layer = tf.keras.layers.Dropout(self.keep_prob)
        self.second_layer = tf.keras.layers.Dense(input_shape[-1],
                                                  kernel_initializer=self.kernel_initializer,
                                                  kernel_regularizer=self.regularizer)
        self.activation2 = self.activation()
        self.norm = tf.keras.layers.BatchNormalization()

    def get_config(self):
        config = {
            'ffn_dim': self.ffn_dim,
            'keep_prob': self.keep_prob,
            'activation': self.activation_name,
            'kernel_regularizer': self.regularizer
        }

        base_config = super(Residual_DNN_block, self).get_config()
        return dict(list(base_config.items()) + list(config.items()))

    def call(self, inputs, training=True):
        outputs = self.first_layer(inputs)
        outputs = self.activation1(outputs)
        outputs = self.dropout_layer(outputs, training=training)
        outputs = self.second_layer(outputs)
        outputs = self.activation2(outputs)
        outputs = self.norm(outputs + inputs, training=training)
        return outputs


class Residual_FC_regressor(tf.keras.Sequential):
    def __init__(self,
                 n_features,
                 ffn_dims=[64, 32, 16],
                 model_dim=140,
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 validation_size=0.15,
                 n_outputs=1,
                 epochs=150,
                 batch_size=32,
                 learning_rate=0.001,
                 loss_function='MSE',
                 l1_beta=0,
                 l2_beta=0,
                 minimum_epoch=20,
                 AE_model=None,
                 AE_params=None,
                 name=None):
        super(Residual_FC_regressor, self).__init__()
        self.epochs = epochs - minimum_epoch
        self.batch_size = batch_size
        self.validation_size = validation_size
        if loss_function == 'MSE':
            self.loss_fn = tf.losses.mean_squared_error
        elif loss_function == 'MAE':
            self.loss_fn = tf.losses.absolute_difference
        elif loss_function == 'RMSE':
            self.loss_fn = rmse_loss
        else:
            self.loss_fn = loss_function
        regularizer = tf.keras.regularizers.l1_l2(l1_beta, l2_beta)
        activation_func = get_activation_for_layers(activation)

        # pretrained Autoencoder ?????? ????????? ?????? ????????? AE_model ??????
        if AE_model is not None:
            self.AE = AE_model(AE_params)  # AE_model ????????? AE_params??? ??????(AE_model ?????? ????????? params ??????)
            self.encode_layer = self.AE.get_encoder_objects()  # AE_model ???????????? encoder ????????? ?????? sequential ????????? ??????????????? method ??????
            self.add(self.encode_layer)  # encoding layer ????????? ?????? ?????? ?????? ??????????????? ?????????

        self.add(tf.keras.layers.Dense(model_dim, kernel_regularizer=regularizer))
        self.add(tf.keras.layers.BatchNormalization())
        self.add(activation_func())

        for block_num in range(len(ffn_dims)):
            self.add(Residual_DNN_block(ffn_dims[block_num], activation, keep_probs[block_num],
                                        kernel_regularizer=regularizer))
        self.add(tf.keras.layers.Dense(n_outputs, kernel_regularizer=regularizer))
        self.optimizer = tf.keras.optimizers.Adam(learning_rate)
        self.compile(self.optimizer, self.loss_fn)

        # ?????? ??????
        self.build([None, n_features])
        # ?????? ????????? ??????
        self.initial_values = self.get_weights()
        self.minimum_epoch = minimum_epoch
        self.params = {
            'n_features': n_features,
            'ffn_dims': ffn_dims,
            'model_dim': model_dim,
            'activation': activation,
            'keep_probs': keep_probs,
            'validation_size': validation_size,
            'n_outputs': n_outputs,
            'epochs': epochs,
            'batch_size': batch_size,
            'learning_rate': learning_rate,
            'loss_function': loss_function,
            'l1_beta': l1_beta,
            'l2_beta': l2_beta,
            'minimum_epoch': minimum_epoch,
            'AE_model': AE_model,
            'AE_params': AE_params,
            'name': name
        }

    def pretrain(self, x, verbose=False):
        self.AE.fit(x, verbose=verbose)

    def train(self, x, y, save_path=None, monitor='val_loss', verbose=True, pre_train=True):

        if pre_train:
            self.AE.fit(x, verbose=verbose)
        if save_path is not None:
            callbacks = [tf.keras.callbacks.ModelCheckpoint(save_path, monitor=monitor, save_best_only=True,
                                                            save_weights_only=True)]
        else:
            callbacks = []
        if self.minimum_epoch:
            self.fit(x, y, epochs=self.minimum_epoch, batch_size=self.batch_size)
        return self.fit(x, y, epochs=self.epochs, batch_size=self.batch_size, validation_split=self.validation_size)

    def predict_saved(self, x, save_path):
        """
        ????????? ??????(save_path) ????????? weight ??????????????? predict
        """

        self.load_weights(save_path)
        return self.predict(x)

    def predict_proba_saved(self, x, save_path):
        """
        ????????? ??????(save_path) ????????? weight ??????????????? predict_proba
        """
        self.load_weights(save_path)
        return self.predict_proba(x)

    def model_save(self, save_path):
        """
        ?????? ??????????????? ??????
        """
        with open(save_path, 'wb') as pi:
            pickle.dump(self.to_json(), pi, protocol=pickle.HIGHEST_PROTOCOL)

    def reset_weights(self):
        """
        ?????? ?????????(????????? initialize)?????? ?????????
        """
        self.set_weights(self.initial_values)


class Residual_FC_classifier(tf.keras.Sequential):
    def __init__(self,
                 n_features,
                 ffn_dims=[64, 32, 16],
                 model_dim=140,
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 validation_size=0.15,
                 n_classes=2,
                 epochs=150,
                 batch_size=32,
                 learning_rate=0.001,
                 loss_function='CR',
                 l1_beta=0,
                 l2_beta=0,
                 minimum_epoch=20,
                 AE_model=None,
                 AE_params=None,
                 name=None):
        super(Residual_FC_classifier, self).__init__()
        self.epochs = epochs - minimum_epoch
        self.minimum_epoch = minimum_epoch
        self.batch_size = batch_size
        self.validation_size = validation_size
        if loss_function == 'CR':
            self.loss_fn = tf.losses.softmax_cross_entropy
        else:
            self.loss_fn = loss_function

        regularizer = tf.keras.regularizers.l1_l2(l1_beta, l2_beta)
        activation_func = get_activation_for_layers(activation)

        # pretrained Autoencoder ?????? ????????? ?????? ????????? AE_model ??????
        if AE_model is not None:
            self.AE = AE_model(AE_params)  # AE_model ????????? AE_params??? ??????(AE_model ?????? ????????? params ??????)
            self.encode_layer = self.AE.get_encoder_objects()  # AE_model ???????????? encoder ????????? ?????? sequential ????????? ??????????????? method ??????
            self.add(self.encode_layer)  # encoding layer ????????? ?????? ?????? ?????? ??????????????? ?????????

        self.add(tf.keras.layers.Dense(model_dim, kernel_regularizer=regularizer))
        self.add(tf.keras.layers.BatchNormalization())
        self.add(activation_func())

        for block_num in range(len(ffn_dims)):
            self.add(Residual_DNN_block(ffn_dims[block_num], activation, keep_probs[block_num],
                                        kernel_regularizer=regularizer))
        self.add(tf.keras.layers.Dense(n_classes, kernel_regularizer=regularizer, activation='softmax'))

        self.optimizer = tf.keras.optimizers.Adam(learning_rate)
        self.compile(self.optimizer, self.loss_fn, metrics=['accuracy'])

        # ?????? ??????
        self.build([None, n_features])
        # ?????? ????????? ??????
        self.initial_values = self.get_weights()

        self.params = {
            'n_features': n_features,
            'ffn_dims': ffn_dims,
            'model_dim': model_dim,
            'activation': activation,
            'keep_probs': keep_probs,
            'validation_size': validation_size,
            'n_classes': n_classes,
            'epochs': epochs,
            'batch_size': batch_size,
            'learning_rate': learning_rate,
            'loss_function': loss_function,
            'l1_beta': l1_beta,
            'l2_beta': l2_beta,
            'minimum_epoch': minimum_epoch,
            'AE_model': AE_model,
            'AE_params': AE_params,
            'name': name
        }

    def pretrain(self, x, verbose=False):
        print('auto_encoder training_start')
        self.AE.fit(x, verbose=verbose)

    def train(self, x, y, save_path=None, monitor='accuracy', verbose=True, pre_train=True):
        if pre_train:
            self.AE.fit(x, verbose=verbose)
        if save_path is not None:
            callbacks = [tf.keras.callbacks.ModelCheckpoint(save_path, monitor=monitor, save_best_only=True,
                                                            save_weights_only=True)]
        else:
            callbacks = []

        if self.minimum_epoch:
            self.fit(x, y, epochs=self.minimum_epoch, batch_size=self.batch_size)
        return self.fit(x, y, epochs=self.epochs, batch_size=self.batch_size, validation_split=self.validation_size,
                        callbacks=callbacks)

    def predict_saved(self, x, save_path):
        """
        ????????? ??????(save_path) ????????? weight ??????????????? predict
        """

        self.load_weights(save_path)
        return self.predict(x)

    def predict_proba_saved(self, x, save_path):
        """
        ????????? ??????(save_path) ????????? weight ??????????????? predict_proba
        """
        self.load_weights(save_path)
        return self.predict_proba(x)

    def model_save(self, save_path):
        """
        ?????? ??????????????? ??????
        """
        with open(save_path, 'wb') as pi:
            pickle.dump(self.to_json(), pi, protocol=pickle.HIGHEST_PROTOCOL)

    def reset_weights(self):
        """
        ?????? ?????????(????????? initialize)?????? ?????????
        """
        self.set_weights(self.initial_values)
