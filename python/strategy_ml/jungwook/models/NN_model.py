import pickle
import tensorflow as tf
import numpy as np
from nn_utils import get_activation_for_layers, rmse_loss, class_iter, batch_iter


class DNN(tf.keras.Sequential):
    def __init__(self,
                 n_features,
                 loss_function='MSE',
                 AE_model=None,
                 AE_params=None,
                 train_entire=True):
        super(DNN, self).__init__()
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
        # pretrained Autoencoder 모델 연결할 경우 지정된 AE_model 사용
        if AE_model is not None:
            self.AE = AE_model(AE_params)  # AE_model 객체는 AE_params로 선언(AE_model 마다 필요한 params 상이)
            self.encode_layer = self.AE.get_encoder_objects()  # AE_model 객체에는 encoder 부분만 따로 sequential 모델로 출력해주는 method 존재
            if self.train_entire:
                self.add(self.encode_layer)  # encoding layer 모델을 전체 모델 가장 앞단에다가 붙여줌

        self.build_main_model()

        if self.train_entire:
            self.build([None, n_features])
        else:
            self.build([None, AE_params['units'][-1]])
        # 모델 초기값 저장
        self.initial_values = self.get_weights()

    def build_main_model(self):
        pass

        # 모델 구축

    def pretrain(self, x, verbose=False):

        if hasattr(self, 'AE'):
            print('auto_encoder training_start')
            self.AE.fit(x, verbose=verbose)
        else:
            print('auto_encoder is not found skip pretrain')
            return

    def train(self, x, y, epochs=1, batch_size=32, save_path=None, eval_set=None, monitor='val_loss', minimum_epoch=0,
              verbose=True, pre_train=True, early_stopping=None):

        if pre_train:
            self.pretrain(x, verbose=verbose)

        if minimum_epoch:
            self.fit(x, y, epochs=minimum_epoch, batch_size=batch_size)
        callbacks = self.set_callback(monitor=monitor, decay_rate=0.1, patience=5, early_stopping=early_stopping,
                                      save_path=save_path)

        return self.fit(x, y, epochs=epochs - minimum_epoch, validation_data=eval_set, batch_size=batch_size,
                        callbacks=callbacks)

    def train_with_sample(self, x, y, batch_size=32, step_limit=10000, eval_set=None, save_path=None, best_path=None,
                          verbose=True, pre_train=True, monitor='val_loss'):
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
                                verbose=True, pre_train=True, monitor='val_loss'):
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
                        if cls_mode:
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

        self.load_weights(save_path)
        return self.predict(x)

    def predict_proba_saved(self, x, save_path):
        """
        저장된 경로(save_path) 로부터 weight 읽어들이고 predict_proba
        """
        self.load_weights(save_path)
        return self.predict_proba(x)

    def model_save(self, save_path):
        """
        모델 아키텍쳐만 저장
        """
        with open(save_path, 'wb') as pi:
            pickle.dump(self.to_json(), pi, protocol=pickle.HIGHEST_PROTOCOL)

    def reset_weights(self):
        """
        모델 초기값(첫번쨰 initialize)으로 되돌림
        """
        self.set_weights(self.initial_values)