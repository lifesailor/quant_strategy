import pickle
import os

os.environ['TF_KERAS'] = '1'
import tensorflow as tf
import numpy as np
from nn_utils import get_activation_for_layers, rmse_loss, class_iter
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
from keras_radam import RAdam


class DNN(tf.keras.Sequential):
    def __init__(self,
                 n_features,
                 loss_function='MSE',
                 AE_model=None,
                 AE_params=None,
                 train_entire=True,
                 radam=True):
        super(DNN, self).__init__()
        if AE_params is not None:
            AE_params['n_features'] = n_features
        self.train_entire = train_entire
        if loss_function == 'CR':
            self.loss_fn = tf.losses.softmax_cross_entropy
        if loss_function == 'MSE':
            self.loss_fn = tf.losses.mean_squared_error
        elif loss_function == 'MAE':
            self.loss_fn = tf.losses.absolute_difference
        elif loss_function == 'RMSE':
            self.loss_fn = rmse_loss
        else:
            self.loss_fn = loss_function
        self.radam = radam
        # pretrained Autoencoder 모델 연결할 경우 지정된 AE_model 사용
        if AE_model is not None:
            self.AE = AE_model(AE_params)  # AE_model 객체는 AE_params로 선언(AE_model 마다 필요한 params 상이)
            self.encode_layer = self.AE.get_encoder_objects()  # AE_model 객체에는 encoder 부분만 따로 sequential 모델로 출력해주는 method 존재
            if self.train_entire:
                self.add(self.encode_layer)  # encoding layer 모델을 전체 모델 가장 앞단에다가 붙여줌

        self.build_main_model()

        self.compile_model()

        if self.train_entire:
            self.build([None, n_features])
        else:
            self.build([None, AE_params['units'][-1]])
        # 모델 초기값 저장
        self.initial_values = self.get_weights()

    def build_main_model(self):
        pass

        # optmizer
    def compile_model(self):
        if self.radam:
            self.optimizer = RAdam(self.learning_rate)
        else:
            self.optimizer = tf.keras.optimizers.Adam(self.leraning_rate)

        self.compile(self.optimizer, self.loss_fn, metrics=[self.metrics_])

    def pretrain(self, x, verbose=False):

        if hasattr(self, 'AE'):
            print('auto_encoder training_start')
            self.AE.fit(x, verbose=verbose)
        else:
            print('auto_encoder is not found skip pretrain')
            return

    def train(self, x, y, epochs=1, batch_size=32, save_path=None, eval_set=None, monitor='val_loss', minimum_epoch=0,
              verbose=False, pre_train=True, early_stopping=None):
        if save_path.split('.')[-1] != 'h5':
           save_path += '.h5'

        if pre_train:
            self.pretrain(x, verbose=verbose)

        if minimum_epoch:
            self.fit(x, y, epochs=minimum_epoch, batch_size=batch_size, verbose = verbose)
        callbacks = self.set_callback(monitor=monitor, decay_rate=0.1, patience=5, early_stopping=early_stopping,
                                      save_path=save_path)

        return self.fit(x, y, epochs=epochs - minimum_epoch, validation_data=eval_set, batch_size=batch_size,
                        callbacks=callbacks, verbose = verbose)

    def train_with_sample(self, x, y, batch_size=32, step_limit=10000, eval_set=None, save_path=None, best_path=None,
                          verbose=False, pre_train=True, monitor='val_loss'):
        if save_path.split('.')[-1] != 'h5':
           save_path += '.h5'

        if pre_train:
            self.pretrain(x, verbose=verbose)
            x_ = x
        if not self.train_entire:
            x_ = self.encode_layer(x)
        if monitor == 'val_loss':
            cls_mode = False
            compare = 0
        elif monitor == 'val_acc':
            cls_mode = True
            compare = 100000
        else:
            raise KeyError("you insert wrong monitor : {}, use 'val_loss' or 'val_acc'".format(monitor))
        eval_mode = False
        step_size = 1
        if eval_set is not None:
            eval_mode = True
            X_val, Y_val = eval_set

        for batch_x, batch_y in batch_iter(x_, y, batch_size=batch_size, shuffle=True):
            c = self.train_on_batch(batch_x, batch_y)

            if step_size % 100 == 0:
                # validation 잇을시
                if eval_mode:
                    val_pred = self.predict_proba(X_val)

                    val_c = self.loss_fn(Y_val, val_pred)
                    if cls_mode:
                        monitor_value = np.mean(np.equal(np.argmax(val_pred, 1), np.argmax(y, 1)))

                    else:
                        monitor_value = -val_c

                    if best_path is not None and step_size > 1000:
                        if monitor_value > compare:
                            compare = monitor_value
                            self.save_weights(best_path)

                if verbose:
                    train_pred = self.predict_proba(x_)
                    train_c = self.loss_fn(y, train_pred)
                    train_text = 'TRAIN] step : {}, cost : {}'.format(step_size, train_c)

                    if cls_mode:
                        train_text += " acc : {}".format(np.mean(np.equal(np.argmax(train_pred, 1), np.argmax(y, 1))))

                    print(train_text)
                    if eval_mode:
                        val_text = 'VAL] step : {}, cost : {}'.format(step_size, val_c)
                        val_text += " acc : {}".format(monitor_value)
                        print(val_text)

            step_size += 1
            if step_size > step_limit:
                break
        self.save_weights(save_path)
        return self

    def train_with_sample_class(self, x, y, sample_numbers, step_limit=10000, eval_set=None, save_path=None,
                                best_path=None,
                                verbose=False, pre_train=True, monitor='val_loss'):
        if pre_train:
            self.pretrain(x, verbose=verbose)
            x_ = x
        if not self.train_entire:
            x_ = self.encode_layer(x)
        if monitor == 'val_loss':
            cls_mode = False
            compare = 0
        elif monitor == 'val_acc':
            cls_mode = True
            compare = 100000
        else:
            raise KeyError("you insert wrong monitor : {}, use 'val_loss' or 'val_acc'".format(monitor))
        eval_mode = False
        step_size = 1
        if eval_set is not None:
            eval_mode = True
            X_val, Y_val = eval_set

        for batch_x, batch_y in class_iter(x_, y, sample_numbers=sample_numbers):
            c = self.train_on_batch(batch_x, batch_y)

            if step_size % 100 == 0:
                # validation 잇을시
                if eval_mode:
                    val_pred = self.predict_proba(X_val)

                    val_c = self.loss_fn(Y_val, val_pred)
                    if cls_mode:
                        monitor_value = np.mean(np.equal(np.argmax(val_pred, 1), np.argmax(y, 1)))

                    else:
                        monitor_value = -val_c

                    if best_path is not None and step_size > 1000:
                        if monitor_value > compare:
                            compare = monitor_value
                            self.save_weights(best_path)

                if verbose:
                    train_pred = self.predict_proba(x_)
                    train_c = self.loss_fn(y, train_pred)
                    train_text = 'TRAIN] step : {}, cost : {}'.format(step_size, train_c)

                    if cls_mode:
                        train_text += " acc : {}".format(np.mean(np.equal(np.argmax(train_pred, 1), np.argmax(y, 1))))

                    print(train_text)
                    if eval_mode:
                        val_text = 'VAL] step : {}, cost : {}'.format(step_size, val_c)
                        val_text += " acc : {}".format(monitor_value)
                        print(val_text)

            step_size += 1
            if step_size > step_limit:
                break
        self.save_weights(save_path)
        return self

    def set_callback(self, monitor, decay_rate=0.1, patience=5, early_stopping=None, save_path=None):
        callbacks = [tf.keras.callbacks.ReduceLROnPlateau(monitor=monitor, patience=patience, factor=decay_rate)]
        if early_stopping is not None:
            callbacks.append(tf.keras.callbacks.EarlyStopping(monitor=monitor, patience=early_stopping))
        if save_path is not None:
            callbacks.append(tf.keras.callbacks.ModelCheckpoint(save_path, monitor=monitor, save_best_only=True,
                                                                save_weights_only=True))
        return callbacks

    def predict_saved(self, x, save_path):
        """
        저장된 경로(save_path) 로부터 weight 읽어들이고 predict
        """
        if save_path.split('.')[-1] != 'h5':
           save_path += '.h5'
        self.load_weights(save_path)

        return self.predict(x)

    def predict_proba_saved(self, x, save_path):
        """
        저장된 경로(save_path) 로부터 weight 읽어들이고 predict_proba
        """
        if save_path.split('.')[-1] != 'h5':
           save_path += '.h5'
        self.load_weights(save_path)


        return self.predict_proba(x)

    def model_save(self, save_path):
        """
        모델 아키텍쳐만 저장
        """
        if save_path.split('.')[-1] != 'json':
           save_path += '.json'
        with open(save_path, 'wb') as pi:
            pickle.dump(self.to_json(), pi, protocol=pickle.HIGHEST_PROTOCOL)

    def reset_weights(self):
        """
        모델 초기값(첫번쨰 initialize)으로 되돌림
        """
        self.set_weights(self.initial_values)


class DNN_regressor(DNN):
    def __init__(self,
                 n_features,
                 hidden_units=[64, 32, 16],
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 batch_norm=[False, False, False],
                 n_outputs=1,
                 learning_rate=0.001,
                 loss_function='MSE',
                 l1_beta=0,
                 l2_beta=0,
                 AE_model=None,
                 AE_params=None,
                 metrics='MSE',
                 radam=True,
                 name=None):

        self.hidden_units = hidden_units
        self.keep_probs = keep_probs
        self.batch_norm = batch_norm
        self.n_classes = n_outputs
        self.activation = activation
        self.l1_beta = l1_beta
        self.l2_beta = l2_beta
        self.learning_rate = learning_rate
        self.metrics_ = metrics
        super(DNN_regressor, self).__init__(n_features=n_features,
                                            loss_function=loss_function,
                                            radam=radam,
                                            AE_model=AE_model,
                                            AE_params=AE_params,
                                            )

    def build_main_model(self):
        activation_func = get_activation_for_layers(self.activation)
        for layer_num in range(len(self.hidden_units)):

            self.add(tf.keras.layers.Dense(self.hidden_units[layer_num], activation_func(),
                                           kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta),
                                           bias_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))
            if self.batch_norm[layer_num]:
                self.add(tf.keras.layers.BatchNormalization())
            self.add(tf.keras.layers.Dropout(self.keep_probs[layer_num]))
        self.add(tf.keras.layers.Dense(self.n_classes))


class DNN_classifier(DNN):
    def __init__(self,
                 n_features,
                 hidden_units=[64, 32, 16],
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 batch_norm=[False, False, False],
                 n_classes=3,
                 learning_rate=0.001,
                 loss_function='CR',
                 l1_beta=0,
                 l2_beta=0,
                 AE_model=None,
                 AE_params=None,
                 metrics='accuracy',
                 radam=True,
                 name=None):
        self.hidden_units = hidden_units
        self.keep_probs = keep_probs
        self.batch_norm = batch_norm
        self.n_classes = n_classes
        self.activation = activation
        self.l1_beta = l1_beta
        self.l2_beta = l2_beta
        self.learning_rate = learning_rate
        self.metrics_ = metrics
        super(DNN_classifier, self).__init__(n_features=n_features,
                                             loss_function=loss_function,
                                             radam=radam,
                                             AE_model=AE_model,
                                             AE_params=AE_params,
                                             )

    def build_main_model(self):
        activation_func = get_activation_for_layers(self.activation)
        for layer_num in range(len(self.hidden_units)):

            self.add(tf.keras.layers.Dense(self.hidden_units[layer_num], activation_func(),
                                           kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta),
                                           bias_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))
            if self.batch_norm[layer_num]:
                self.add(tf.keras.layers.BatchNormalization())
            self.add(tf.keras.layers.Dropout(self.keep_probs[layer_num]))
        if self.n_classes == 1:
            activation = 'sigmoid'
        else:
            activation = 'softmax'
        self.add(tf.keras.layers.Dense(self.n_classes, activation=activation))
        #         self.optimizer = tf.keras.optimizers.Adam(self.learning_rate)
        self.optimizer = RAdam(self.learning_rate)
        self.compile(self.optimizer, self.loss_fn, metrics=[self.metrics_])


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
                                                 bias_regularizer=self.regularizer,
                                                 kernel_regularizer=self.regularizer)
        self.activation1 = self.activation()
        self.dropout_layer = tf.keras.layers.Dropout(self.keep_prob)
        self.second_layer = tf.keras.layers.Dense(input_shape[-1],
                                                  kernel_initializer=self.kernel_initializer,
                                                  bias_regularizer=self.regularizer,
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


class Residual_FC_regressor(DNN):
    def __init__(self,
                 n_features,
                 ffn_dims=[64, 32, 16],
                 model_dim=140,
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 learning_rate=0.001,
                 n_outputs=1,
                 loss_function='MSE',
                 l1_beta=0,
                 l2_beta=0,
                 AE_model=None,
                 AE_params=None,
                 metrics='MSE',
                 radam= True,
                 name=None):
        self.model_dim = model_dim
        self.ffn_dims = ffn_dims
        self.keep_probs = keep_probs
        self.n_classes = n_outputs
        self.activation = activation
        self.l1_beta = l1_beta
        self.l2_beta = l2_beta
        self.learning_rate = learning_rate
        self.metrics_ = metrics
        super(Residual_FC_regressor, self).__init__(n_features=n_features,
                                                    loss_function=loss_function,
                                                    radam=radam,
                                                    AE_model=AE_model,
                                                    AE_params=AE_params,
                                                    )

    def build_main_model(self):
        activation_func = get_activation_for_layers(self.activation)
        self.add(tf.keras.layers.Dense(self.model_dim,
                                       kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta),
                                       bias_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))
        self.add(tf.keras.layers.BatchNormalization())
        self.add(activation_func())

        for block_num in range(len(self.ffn_dims)):
            self.add(Residual_DNN_block(self.ffn_dims[block_num], self.activation, self.keep_probs[block_num],
                                        kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))

        self.add(tf.keras.layers.Dense(self.n_classes))





class Residual_FC_classifier(DNN):
    def __init__(self,
                 n_features,
                 ffn_dims=[64, 32, 16],
                 model_dim=140,
                 activation='relu',
                 keep_probs=[0.5, 0.5, 0.5],
                 learning_rate=0.001,
                 n_classes=2,
                 loss_function='CR',
                 l1_beta=0,
                 l2_beta=0,
                 AE_model=None,
                 AE_params=None,
                 metrics='accuracy',
                 radam=True,
                 name=None):
        self.model_dim = model_dim
        self.ffn_dims = ffn_dims
        self.keep_probs = keep_probs
        self.n_classes = n_classes
        self.activation = activation
        self.l1_beta = l1_beta
        self.l2_beta = l2_beta
        self.learning_rate = learning_rate
        self.metrics_ = metrics
        super(Residual_FC_classifier, self).__init__(n_features=n_features,
                                                     loss_function=loss_function,
                                                     radam=radam,
                                                     AE_model=AE_model,
                                                     AE_params=AE_params,
                                                     )

    def build_main_model(self):
        activation_func = get_activation_for_layers(self.activation)
        self.add(tf.keras.layers.Dense(self.model_dim,
                                       kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta),
                                       bias_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))
        self.add(tf.keras.layers.BatchNormalization())
        self.add(activation_func())

        for block_num in range(len(self.ffn_dims)):
            self.add(Residual_DNN_block(self.ffn_dims[block_num], self.activation, self.keep_probs[block_num],
                                        kernel_regularizer=tf.keras.regularizers.l1_l2(self.l1_beta, self.l2_beta)))
        if self.n_classes == 1:
            activation = 'sigmoid'
        else:
            activation = 'softmax'
        self.add(tf.keras.layers.Dense(self.n_classes, activation=activation))


# if __name__ == '__main__':
#     tf.enable_eager_execution()
#     AE_params = {
#         'units': [64, 32, 16],
#         'batch_norm': [False, False, False],
#         'activation': tf.nn.sigmoid,
#         'learning_rate': 0.001,
#         'batch_size': 128,
#         'epochs': 100,
#         'n_features': 30,  # features
#         'noise_ratio': 0.20,
#         'loss_function': 'MSE'
#     }
#     from DAE import DAE
#     import numpy as np
#
#     x = np.random.normal(size=(1000, 30))
#     y = np.eye(4)[np.random.randint(0, 4, size=(1000,))]
#
#     activate = DNN_classifier(n_features=30, n_classes=4, AE_model=DAE, AE_params=AE_params)
#     print(activate.summary())
#     print(activate.AE.get_encoder_objects()(x))
#     activate.train(x, y)