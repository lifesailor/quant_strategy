import os
import re

import tensorflow as tf
import numpy as np
from tqdm import tqdm


def nll_loss(var, y_true, y_pred):
    mean = y_pred
    sigma = tf.log(1. + tf.exp(var))
    return 0.5 * tf.reduce_mean(tf.log(sigma)) + 0.5 * tf.reduce_mean(
        tf.div(tf.square(y_true - mean), sigma)) + 0.5 * tf.log(2 * np.pi)


def rmse_loss(y_true, y_pred):
    eps = 1e-5
    return tf.reduce_mean(tf.sqrt(tf.abs(y_true - y_pred) + eps))


def cr_mse_loss(y_true, logits, matrix):
    """
    loss for label that has order for good thing
    :param y_true: label: would be onehot encoded
    :param logits: logits: It should be logits(float), not prediction(int)
    :param matrix: k-y matrix
    :return: loss
    """
    # prob 구하기
    prob = tf.nn.softmax(logits)
    # 예측 클래스
    prediction = tf.argmax(prob, 1)
    # onehot encoding 해제
    label = tf.argmax(y_true, 1)


class Early_stopping:
    '''
    Early_stopping)
        특정 에포크동안 계속 loss 가 상승하면 training stop

    Method)
        constructor_params :
                 - patience : 허용하는 에포크 수, patience 이상 연속적으로 loss가 상승하면 ealry stop
        validate : loss를 파라미터로 받아서 early stop을 할 것인지 여부 결정
        - (inputs : loss --> outputs : True or False )
    '''

    def __init__(self, patience=0):
        # step: 연속적으로 증가한 횟수
        self._step = 0
        # loss : 바로 전 loss (초기값 : inf)
        self._measure = np.inf
        self.patience = patience

    def __call__(self, measure):
        '''
        minimize measure
        '''
        # loss 함수가 바로 전 loss 함수보다 클경우 step 업데이트
        if self._measure < measure:
            self._step += 1
            # 연속적으로 증가한 횟수가 설정한 patience보다 높으면 early stop
            if self._step >= self.patience:
                print('Early stop training')
                return True
        # loss 함수가 바로 전 loss 함수보다 작을경우 step 초기화 및 loss 업데이트
        else:
            self._step = 0
            self._measure = measure
        return False


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
    return


class activator:
    '''
    Activator)
        Model과 Parameter들을 받아서 모델 훈련/실행


    Method)
        build_net : activator 객체 생성후 실행하면 신경망 생성
        fit : Train 메소드
         - (inputs : 훈련 데이터, 훈련 타겟, 테스트 데이터, 테스트 타겟 )
        predict : 예측값 생성 메소드
         - (Inputs : 데이터, 타겟, Restore(True면 pretrained model load) -> outputs : 예측값)
        predict_proba : (Classification 문제일경우 사용) 각 클래스 분류의 확률값 생성
         - (Inputs : 데이터, 타겟, Restore(True면 pretrained model load) -> outputs : 예측확률값)


    params 필수요소)
     input_dimension : input으로 들어갈 차원 (cnn은 4차원, rnn은 3차원)
     learning_rate : 시작 learning_rate
     decay : learning rate 떨어지는 비율 (20000step에 한번)
     decay_steps : 20000

     height : 두번째 차원(sequential이면 seq_length, image면 세로 차원)
     width : 세번째 차원(sequential이면 n_features, image면 가로 차원)
     MSE : True면 Regression, False면 Classification
     num_classes : 예측할 label의 갯수
     batch_size : 미니 배치 사이즈
     epochs : 훈련 에폭 수
     shuffle_per_epoch : True면 매 에폭마다 shuffle
     optimizer_kinds : 'Adam' or 'SGD'
     momentum : SGD일경우 모멘텀 수치

     model_path : 모델이 저장될 디렉토리
     restore : True면 pretrain된 모델을 불러움
     tensorboard_path : 텐서보드 이벤트파일이 저장되는 디렉토리

    '''

    def __init__(self, model, params, name, RBM_model, RBM_params):
        self.RBM_params = RBM_params
        self.RBM_model = RBM_model
        self.input_dimension = params['input_dimension']
        self.height = params['height']
        self.width = params['width']
        self.regression = params['regression']
        self.loss_function = params['loss']

        if self.loss_function == 'MSE':
            self.loss_fn = tf.losses.mean_squared_error
        elif self.loss_function == 'MAE':
            self.loss_fn = tf.losses.absolute_difference
        elif self.loss_function == 'CR':
            self.loss_fn = tf.losses.softmax_cross_entropy
        elif self.loss_function == 'RMSE':
            self.loss_fn = rmse_loss
        elif self.loss_function == 'NLL':
            self.loss_fn = nll_loss

        self.num_classes = params['num_classes']
        self.boundary = params['boundary']
        self.extra_task = params['extra_task']
        self.regression_extra = params['regression_extra']
        self.loss_extra = params['loss_extra']
        if self.loss_extra == 'MSE':
            self.loss_fn_extra = tf.losses.mean_squared_error
        elif self.loss_extra == 'MAE':
            self.loss_fn_extra = tf.losses.absolute_difference
        elif self.loss_extra == 'CR':
            self.loss_fn_extra = tf.losses.softmax_cross_entropy
        elif self.loss_extra == 'RMSE':
            self.loss_fn_extra = rmse_loss
        elif self.loss_extra == 'NLL':
            self.loss_fn_extra = nll_loss

        self.batch_size = params['batch_size']
        self.epochs = params['epochs']
        self.shuffle_per_epoch = params['shuffle_per_epoch']
        self.optimizer_kinds = params['optimizer_kinds']
        self.learning_rate = params['learning_rate']
        self.decay = params['decay']
        self.decay_step = params['decay_step']
        self.momentum = params['momentum']
        self.l1_beta = params['l1_beta']
        self.l2_beta = params['l2_beta']

        self.RBM_ = params['RBM']
        self.model_path = params['model_path']
        self.restore = params['restore']
        self.restore_best = params['restore_best']
        self.validation = params['validation']

        self.tensorboard_path = params['tensorboard_path']
        self.save_path = params['save_path']
        self.patience = params['patience']
        self.minimum_epoch = params['minimum_epoch']
        self.save = params['save']
        self.name = name
        self.model = model(params)
        self.best_factor = None
        self.num_classes2 = params['num_classes2']

    def build_net(self, sess):
        self.output_dense = tf.layers.Dense(self.num_classes,
                                            bias_initializer=tf.random_uniform_initializer(minval=-self.boundary,
                                                                                           maxval=self.boundary))
        self.output_dense_var = tf.layers.Dense(self.num_classes)
        if self.extra_task:
            self.output_dense2 = tf.layers.Dense(self.num_classes2,
                                                 bias_initializer=tf.random_uniform_initializer(minval=-self.boundary,
                                                                                                maxval=self.boundary))
            self.output_dense_extra_var = tf.layers.Dense(self.num_classes2)
        self.X = tf.placeholder(tf.float32, [None, self.height, self.width], name='data')

        [a, b, c] = self.X.get_shape().as_list()
        if self.input_dimension == 2:
            self.X_input = tf.reshape(self.X, [-1, b * c])
        elif self.input_dimension == 3:
            self.X_input = tf.reshape(self.X, [-1, b, c])
        else:
            self.X_input = tf.expand_dims(self.X, -1)

        self.Y = tf.placeholder(tf.float32, [None, self.num_classes], name='target')

        self.global_step = tf.Variable(0, name="global_step", trainable=False)
        # train net_work
        if self.RBM_:
            # self.RBM = self.RBM_model(n_features = self.RBM_params['n_features'],
            #                           units=self.RBM_params['units'],
            #                           batch_norm=self.RBM_params['batch_norm'],
            #                           activation=self.RBM_params['activation'],
            #                           learning_rate=self.RBM_params['learning_rate'],
            #                           batch_size=self.RBM_params['batch_size'],
            #                           loss_function=self.RBM_params['loss_function'],
            #                           epochs=self.RBM_params['epochs'],
            #                           name=self.name)
            self.RBM = self.RBM_model(self.RBM_params,
                                      name=self.name)
            self.RBM._build_net()
            self.X_input = self.RBM(self.X_input)

        with tf.variable_scope(self.name):

            self.logits_train = self.model(self.X_input)
            self.logits = self.output_dense(self.logits_train)

            if self.loss_function == 'NLL':
                self.logits_var = self.output_dense_var(self.logits_train)
                loss = self.loss_fn(self.logits_var, self.Y, self.logits)
            else:

                loss = self.loss_fn(self.Y, self.logits)
            # if self.loss_fn == 'MSE':
            #     loss = tf.losses.mean_squared_error(self.Y, self.logits)
            # elif self.loss_fn == 'MAE':
            #     loss = tf.losses.absolute_difference(self.Y, self.logits)
            # else:
            #     loss = tf.losses.softmax_cross_entropy(self.Y, self.logits)
            # for multi tasking

            if self.extra_task:
                self.Y2 = tf.placeholder(tf.float32, [None, self.num_classes2], name='target2')
                self.logits_extra = self.output_dense2(self.logits_train)
                if self.loss_extra == 'NLL':
                    self.logits_extra_var = self.output_dense_extra_var(self.logits_train)
                    loss += self.loss_fn_extra(self.logits_extra_var, self.Y2, self.logits_extra)
                else:
                    loss += self.loss_fn_extra(self.Y2, self.logits_extra)

            # L2 Regularization if class has l2_beta
            if self.l1_beta:
                # l1_loss = tf.add_n([tf.reduce_sum(tf.abs(v)) for v in tf.trainable_variables() if
                #                     'bias' not in v.name and 'AE' not in v.name])
                # after tensorflow 1.12.0, all weights related with normalization also in trainable variables, gamma beta except it as well
                l1_loss = tf.add_n([tf.reduce_sum(tf.abs(v)) for v in tf.trainable_variables() if
                                    'Adam' not in v.name and 'gamma' not in v.name and 'beta' not in v.name and 'bias' not in v.name and 'AE' not in v.name])
            else:
                l1_loss = 0

            if self.l2_beta:
                # l2_loss = tf.add_n(
                #     [tf.nn.l2_loss(v) for v in tf.trainable_variables() if 'bias' not in v.name and 'AE' not in v.name])
                # after tensorflow 1.12.0, all weights related with normalization also in trainable variables, gamma beta except it as well
                l2_loss = tf.add_n([tf.nn.l2_loss(v) for v in tf.trainable_variables() if
                                    'Adam' not in v.name and 'gamma' not in v.name and 'beta' not in v.name and 'bias' not in v.name and 'AE' not in v.name])
            else:
                l2_loss = 0
            self.loss = loss + self.l1_beta * l1_loss + self.l2_beta * l2_loss

            learning_rate = tf.train.exponential_decay(self.learning_rate, self.global_step, self.decay_step,
                                                       self.decay, staircase=True)
            update_ops = tf.get_collection(tf.GraphKeys.UPDATE_OPS, scope=self.name)
            with tf.control_dependencies(update_ops):
                if self.optimizer_kinds == 'Adam':
                    self.optimizer = tf.train.AdamOptimizer(learning_rate).minimize(self.loss,
                                                                                    global_step=self.global_step,
                                                                                    name='optimizer')
                else:
                    self.optimizer = tf.train.MomentumOptimizer(self.learning_rate,
                                                                self.momentum,
                                                                use_nesterov=True).minimize(self.loss,
                                                                                            global_step=self.global_step,
                                                                                            name='optimizer')

            # evaluate net_work
            if not self.regression:
                self.train_prediction = tf.argmax(tf.nn.softmax(self.logits), 1)
                # self.train_accuracy = tf.metrics.accuracy(tf.argmax(self.Y, 1), self.train_prediction, name = 'Accuracy')
                self.train_accuracy = tf.reduce_mean(
                    tf.cast(tf.equal(tf.argmax(self.Y, 1), self.train_prediction), tf.float32))
            if self.extra_task:
                self.train_prediction_extra = tf.argmax(tf.nn.softmax(self.logits_extra), 1)
                self.train_accuracy_extra = tf.reduce_mean(
                    tf.cast(tf.equal(tf.argmax(self.Y2, 1), self.train_prediction_extra), tf.float32))

            self.logits_eval = self.model(self.X_input, is_training=False, reuse=True)

            self.logits_eval_ = self.output_dense(self.logits_eval)
            if self.loss_function == 'NLL':
                self.logits_eval_var = self.output_dense_var(self.logits_train)
                loss_eval = self.loss_fn(self.logits_eval_var, self.Y, self.logits_eval_)
            else:
                loss_eval = self.loss_fn(self.Y, self.logits_eval_)

            if self.extra_task:
                self.logits_eval_extra = self.output_dense2(self.logits_eval)
                if self.loss_extra == 'NLL':
                    self.logits_eval_extra_var = self.output_dense_extra_var(self.logits_eval)
                    loss_eval += self.loss_fn_extra(self.logits_eval_extra_var, self.Y2, self.logits_eval_extra)
                else:
                    loss_eval += self.loss_fn_extra(self.Y2, self.logits_eval_extra)

                if self.regression_extra:
                    self.prediction_extra = tf.identity(self.logits_eval_extra, 'prediction_extra')
                else:
                    self.logits_eval_extra = tf.identity(self.logits_eval_extra, 'logits_extra')
                    self.predict_proba_extra = tf.nn.softmax(self.logits_eval_extra)
                    self.prediction_extra = tf.argmax(self.predict_proba_extra, -1)
                    self.accuracy_extra = tf.reduce_mean(
                        tf.cast(tf.equal(tf.argmax(self.Y2, -1), self.prediction_extra), tf.float32))
            if not self.regression:
                self.logits_eval_ = tf.identity(self.logits_eval_, 'logits')
                self.predict_proba_ = tf.nn.softmax(self.logits_eval_)
                self.prediction = tf.argmax(self.predict_proba_, -1)
                self.accuracy = tf.reduce_mean(tf.cast(tf.equal(tf.argmax(self.Y, -1), self.prediction), tf.float32))
            else:
                self.prediction = tf.identity(self.logits_eval_, name='logits')
            self.loss_eval = loss_eval

        # accuracy = tf.metrics.accuracy(tf.argmax(Y, 1), prediction)

        for i, v in enumerate(tf.trainable_variables()):
            if self.tensorboard_path:
                tf.summary.histogram('Var_{}'.format(v.name), v)
            print('number : {} )) {}'.format(i, v))

        self.saver = tf.train.Saver()
        # 변수들 프린트/ 텐서보드 summary 생성
        if self.tensorboard_path:
            tf.summary.scalar('loss', self.loss)
            if not self.regression:
                tf.summary.scalar('accuracy', self.accuracy)
            self.merged = tf.summary.merge_all()

        if self.restore:
            if self.restore_best:
                self.saver.restore(sess, self.saved_path + 'best/')
            else:
                self.saver.restore(sess, self.saved_path)
        else:
            sess.run(tf.global_variables_initializer())
            sess.run(tf.local_variables_initializer())


    def set_save_path(self, model_name, model_num, date):

        self.save_path = r'./training_result/{}/saved/{}/{}/'.format(model_name,
                                                                model_num,
                                                                start_date.strftime('%Y%m%d'))
        # 저장용 directory 생성
        try:
            os.mkdir(r'./training_result/{}/saved/{}/'.format(model_name, model_num))
        except FileExistsError:
            print('Caution : Model directory already exists')
            pass

        try:
            os.mkdir(save_path)
        except FileExistsError:
            print('Caution : Model directory already exists')
            pass

        #     try:
        #         os.mkdir(params['save_path']+'best/')
        #     except FileExistsError:
        #         print('Caution : Model directory already exists')
        #         pass

    def fit(self, sess, X_train, y_train, X_eval, y_eval, macro_data, y2_train=None, y2_eval=None, verbose=True):
        if self.RBM_:
            # scaler = MinMaxScaler()
            # print(macro_data)
            # macro_data = scaler.fit_transform(macro_data)]
            # input macro_data should have shape (?, n_features)
            self.RBM.fit(sess, macro_data)
        early_stop = Early_stopping(self.patience)
        if self.tensorboard_path:
            writer = tf.summary.FileWriter(self.model_path + self.tensorboard_path, sess.graph)

        try:
            self.val_loss = []
            self.val_acc = []
            for epoch in range(self.epochs):
                total_cost = 0
                total_test_acc = 0
                total_test_acc2 = 0
                # train
                acc = 0
                cost = 0

                acc2 = 0

                total_batch = int(len(X_train) / self.batch_size) + 1
                if self.extra_task:
                    for batch_xs, batch_ys, batch_ys2 in batch_iter(X_train, y_train, y2_train,
                                                                    batch_size=self.batch_size,
                                                                    shuffle=self.shuffle_per_epoch):
                        step = sess.run(self.global_step)
                        train_feed_dict = {self.X: batch_xs, self.Y: batch_ys, self.Y2: batch_ys2}

                        if not self.regression:
                            acc += sess.run(self.train_accuracy, feed_dict=train_feed_dict)
                        if not self.regression_extra:
                            acc2 += sess.run(self.train_accuracy_extra, feed_dict=train_feed_dict)
                        _, c = sess.run([self.optimizer, self.loss], feed_dict=train_feed_dict)

                        cost += c
                        # if not self.regression:
                        if self.tensorboard_path:
                            summ_ = sess.run(self.merged, feed_dict=train_feed_dict)
                            writer.add_summary(summ_, step)

                    if not self.regression_extra:
                        extra_text = ' train_acc2: {}'.format(np.round(acc2 / total_batch, 2))
                    else:
                        extra_text = ''

                    if self.regression and verbose:
                        print('TRAIN) Epoch : {}, cost: {:.4f}'.format(epoch, cost / total_batch) + extra_text)

                    elif verbose:
                        print('TRAIN) Epoch : {}, cost: {:.4f}, train_acc: {}'.format(epoch,
                                                                                      cost / total_batch,
                                                                                      np.round(acc / total_batch,
                                                                                               2)) + extra_text)

                    # test

                    for batch_xs, batch_ys, batch_ys2 in batch_iter(X_eval, y_eval, y2_eval, batch_size=self.batch_size,
                                                                    shuffle=False):
                        total_batch = int(len(X_eval) / self.batch_size) + 1
                        eval_feed_dict = {self.X: batch_xs, self.Y: batch_ys, self.Y2: batch_ys2}
                        if not self.regression:
                            total_test_acc += sess.run(self.accuracy, feed_dict=eval_feed_dict)
                        if not self.regression_extra:
                            total_test_acc2 += sess.run(self.accuracy_extra, feed_dict=eval_feed_dict)
                        total_cost += sess.run(self.loss_eval, feed_dict=eval_feed_dict)

                    test_c = total_cost / total_batch
                    test_acc = total_test_acc / total_batch

                    test_acc2 = total_test_acc2 / total_batch

                    self.val_loss.append(test_c)

                    self.val_acc.append(test_acc)

                    if not self.regression_extra:
                        extra_text = ', acc2: {}'.format(np.round(test_acc2 / total_batch, 2))
                    else:
                        extra_text = ''
                    if self.regression and verbose:
                        print('TEST) Epoch : {}, cost : {:.4f}'.format(epoch, test_c) + extra_text)
                    elif verbose:
                        print('TEST) Epoch : {}, cost : {:.4f}, acc: {}'.format(epoch,
                                                                                test_c,
                                                                                np.round(test_acc, 2)) + extra_text)

                else:
                    for batch_xs, batch_ys in batch_iter(X_train, y_train,
                                                         batch_size=self.batch_size,
                                                         shuffle=self.shuffle_per_epoch):
                        step = sess.run(self.global_step)
                        train_feed_dict = {self.X: batch_xs, self.Y: batch_ys}

                        if not self.regression:
                            acc += sess.run(self.train_accuracy, feed_dict=train_feed_dict)
                        _, c = sess.run([self.optimizer, self.loss], feed_dict=train_feed_dict)

                        cost += c
                        # if not self.regression:
                        if self.tensorboard_path:
                            summ_ = sess.run(self.merged, feed_dict=train_feed_dict)
                            writer.add_summary(summ_, step)
                    if self.regression and verbose:
                        print('TRAIN) Epoch : {}, cost: {:.4f}'.format(epoch, cost / total_batch))

                    elif verbose:
                        print('TRAIN) Epoch : {}, cost: {:.4f}, train_acc: {}'.format(epoch, cost / total_batch,
                                                                                      np.round(acc / total_batch, 2)))

                    # test

                    for batch_xs, batch_ys in batch_iter(X_eval, y_eval, batch_size=self.batch_size, shuffle=False):
                        total_batch = int(len(X_eval) / self.batch_size) + 1
                        eval_feed_dict = {self.X: batch_xs, self.Y: batch_ys}
                        if not self.regression:
                            total_test_acc += sess.run(self.accuracy, feed_dict=eval_feed_dict)

                        total_cost += sess.run(self.loss_eval, feed_dict=eval_feed_dict)

                    test_c = total_cost / total_batch
                    test_acc = total_test_acc / total_batch

                    self.val_loss.append(test_c)
                    self.val_acc.append(test_acc)

                    if self.regression and verbose:
                        print('TEST) Epoch : {}, cost : {:.4f}'.format(epoch, test_c))
                    elif verbose:
                        print('TEST) Epoch : {}, cost : {:.4f}, acc: {}'.format(epoch,
                                                                                test_c,
                                                                                np.round(test_acc, 2)))

                # model should train at least minimum epoch
                # save model to get best model (lowest validation loss)
                if epoch >= self.minimum_epoch:
                    # save model to get best model (lowest validation loss)
                    if self.regression:
                        if min(self.val_loss[self.minimum_epoch:]) >= test_c:
                            self.best_factor = sess.run(self.logits_eval, feed_dict=eval_feed_dict)
                            # print(self.best_factor)
                            if self.save:
                                #save = str(np.round(min(self.val_loss[self.minimum_epoch:]), 3)).split('.')
                                # save = '_'.join(save)
                                self.saver.save(sess,
                                                self.save_path + '/best/' + self.name)# + '_epoch{}_{}'.format(epoch, save))
                    else:
                        if max(self.val_acc[self.minimum_epoch:]) <= test_acc:
                            self.best_factor = sess.run(self.logits_eval, feed_dict=eval_feed_dict)
                            if self.save:
                                # save = str(np.round(max(self.val_acc[self.minimum_epoch:]), 3)).split('.')
                                # save = '_'.join(save)
                                self.saver.save(sess,
                                                self.save_path + '/best/' + self.name)# + '_epoch{}_{}'.format(epoch, save))


        except KeyboardInterrupt:
            print('early stopped in epoch : {}'.format(epoch))
            self.epochs = epoch
        if self.save:
            self.saver.save(sess, self.save_path + self.name, global_step=sess.run(self.global_step))
            print('model saved in path : {}'.format(self.save_path + self.name))
        return self

    def predict(self, sess, data, extra=False, restore=False, restore_best=True, build_network=False, verbose=True):
        total_batch = int(np.ceil(len(data) / self.batch_size))
        data_iter = batch_iter(data, batch_size=self.batch_size, shuffle=False)

        pbar = tqdm(range(total_batch)) if verbose else range(total_batch)
        if restore:
            sess.run(tf.global_variables_initializer())
            if restore_best:
                saved_path = tf.train.latest_checkpoint(self.save_path + 'best/')
            else:
                saved_path = tf.train.latest_checkpoint(self.save_path)
            if saved_path:
                if build_network:
                    print('saved model found :', saved_path)
                    saver = tf.train.import_meta_graph(saved_path + '.meta')
                    print('network loaded')
                    saver.restore(sess, saved_path)
                else:
                    print('saved model found :', saved_path)
                    saver = tf.train.Saver()
                    saver.restore(sess, saved_path)
            else:
                print('can not find saved model!! terminate this function')
                return

            if build_network:
                prediction_reg = r'prediction'
                logits_reg = r'logits'
                if extra:
                    prediction_reg += '_extra'
                    logits_reg += '_extra'

                graph = tf.get_default_graph()
                for op in graph.get_operations():
                    # 모델 결과 도출
                    if re.search(prediction_reg, op.name):
                        pred_name = op.name + ':0'
                        pred = graph.get_tensor_by_name(pred_name)

                    elif re.search(logits_reg, op.name):
                        logit_name = op.name + ':0'
                        logits = graph.get_tensor_by_name(logit_name)
                        pred = tf.argmax(tf.nn.softmax(logits), -1)
                X = graph.get_tensor_by_name('data:0')

                prediction = []
                for p in pbar:
                    batch_xs = next(data_iter)
                    prediction.append(sess.run(pred, feed_dict={X: batch_xs}))
                    pbar.set_description('predict_processing')
                if len(prediction[0].shape) < 2:
                    return np.hstack(prediction)
                return np.vstack(prediction)

        prediction = []
        for p in pbar:
            batch_xs = next(data_iter)
            if extra:
                if self.regression_extra:
                    prediction.append(np.array(sess.run(self.prediction_extra, feed_dict={self.X: batch_xs})))
                else:
                    prediction.append(np.array(sess.run(self.prediction_extra, feed_dict={self.X: batch_xs})).T)
            else:
                if self.regression:
                    prediction.append(np.array(sess.run(self.prediction, feed_dict={self.X: batch_xs})))
                else:
                    prediction.append(np.array(sess.run(self.prediction, feed_dict={self.X: batch_xs})).T)
            if verbose:
                pbar.set_description('predict_processing')
            # print(np.array(sess.run(self.prediction, feed_dict={self.X: batch_xs})).shape)
        # print(prediction)

        if len(prediction[0].shape) < 2:
            return np.hstack(prediction)
        return np.vstack(prediction)

    #
    # def build_network(self, sess, saved_path, legacy):
    #     print('saved model found :', saved_path)
    #     saver = tf.train.import_meta_graph(saved_path + '.meta')
    #     print('network loaded')
    #     saver.restore(sess, saved_path)
    #     prediction_reg = r'prediction'
    #     logits_reg = r'logits'
    #     if legacy:
    #         prediction_reg += '_extra'
    #         logits_reg += '_extra'
    #     graph = tf.get_default_graph()
    #     for op in graph.get_operations():
    #         # 모델 결과 도출
    #         if re.search(prediction_reg, op.name):
    #             raise ValueError('It seems restored network was for MSE that mean there is no predict proba exist')
    #         elif re.search(logits_reg, op.name):
    #             logit_name = op.name + ':0'
    #
    #     X = graph.get_tensor_by_name('data:0')
    #     # y = graph.get_tensor_by_name('target:0')
    #
    #     logits = graph.get_tensor_by_name(logit_name)
    #     predict_proba_ = tf.nn.softmax(logits)
    #     return X, logits

    def predict_proba(self, sess, data, extra=False, restore=False, restore_best=True, build_network=False,
                      verbose=True):
        total_batch = int(np.ceil(len(data) / self.batch_size))
        data_iter = batch_iter(data, batch_size=self.batch_size, shuffle=False)

        pbar = tqdm(range(total_batch)) if verbose else range(total_batch)
        if restore:
            sess.run(tf.global_variables_initializer())
            if restore_best:
                saved_path = tf.train.latest_checkpoint(self.save_path + '/best/')
            else:
                saved_path = tf.train.latest_checkpoint(self.save_path)
            if saved_path:
                if build_network:
                    print('saved model found :', saved_path)
                    saver = tf.train.import_meta_graph(saved_path + '.meta')
                    print('network loaded')
                    saver.restore(sess, saved_path)
                else:
                    print('saved model found :', saved_path)
                    saver = tf.train.Saver()
                    saver.restore(sess, saved_path)
            else:
                print('can not find saved model!! terminate this function')
                return
            if build_network:
                prediction_reg = r'prediction'
                logits_reg = r'logits'
                if extra:
                    prediction_reg += '_extra'
                    logits_reg += '_extra'

                graph = tf.get_default_graph()
                for op in graph.get_operations():
                    # 모델 결과 도출
                    if re.search(prediction_reg, op.name):
                        raise ValueError(
                            'It seems restored network was for MSE that mean there is no predict proba exist')
                    elif re.search(logits_reg, op.name):
                        logit_name = op.name + ':0'

                self.X = graph.get_tensor_by_name('data:0')
                # y = graph.get_tensor_by_name('target:0')

                logits = graph.get_tensor_by_name(logit_name)
                self.predict_proba_ = tf.nn.softmax(logits)

        prediction = []
        for p in pbar:
            batch_xs = next(data_iter)
            if extra:
                prediction.append(sess.run(self.predict_proba_extra, feed_dict={self.X: batch_xs}))
            else:
                prediction.append(sess.run(self.predict_proba_, feed_dict={self.X: batch_xs}))
            pbar.set_description('predict_processing')

        return np.vstack(prediction)


if __name__ == '__main__':
    from utils import prelu

    CRNN_params = {

        # cnn 파라미터
        'use_cnn': False,
        'num_filters': [64, 32],
        'filter_size': [[1, 3], [1, 3]],
        'cnn_batch_norm': [True, True],
        'pool_sizes': [1, 2],
        'cnn_dropout_keep_prob': [0, 0.2],

        # dense 파라미터
        'use_fc': True,
        'fc_hidden_units': [128, 64],
        'fc_batch_norm': [True, True],
        'fc_dropout_keep_prob': [0.1, 0.3],

        # rnn(lstm) 파라미터
        'use_rnn': False,
        'rnn_n_hiddens': [256, 128],
        'rnn_dropout_keep_prob': [0.8, 0.75],
        'use_layer_normalization': True,
        'bi_rnn': False,
        # Global Average Pooling
        'use_gap': False
    }
    # parameter 설정 for residual_fc model
    Residual_FC_params = {
        'ffn_hidden_units': [128, 32, 64],
        'model_dim': 128,
        'ffn_keep_probs': [0.1, 0.2, 0.3]
    }
    ############################################################
    params = {
        'regression': True,
        'loss': 'MAE',
        'learning_rate': 0.001,
        'input_dimension': 2,  # input으로 들어갈 차원 (cnn은 4차원, rnn은 3차원)
        'optimizer_kinds': 'Adam',  # 'Adam or SGD'
        'decay': 0.7,
        'decay_step': 10000,
        'momentum': 0.95,
        'activation': prelu,
        'batch_size': 64,
        'height': 1,  # windowing_size
        'width': 54,  # features
        'num_classes': 2,
        'num_task': 1,

        'extra_task': False,
        'regression_extra': True,
        'loss_extra': 'MAE',
        'num_classes2': 1,

        'shuffle_per_epoch': True,
        'validation': 0,
        'batch_norm_first': True,
        'save': True,
        'restore': False,
        'restore_best': False,
        'l1_beta': 0.,
        'l2_beta': 0.001,
        'lambda': 0,
        'epochs': 150,
        'patience': 10000,
        'minimum_epoch': 30,
        #     'save_path': './training_result/NN4/saved',
        #     'model_path': './training_result/NN4/',
        'tensorboard_path': None,
        'model_file': 'self',
        'RBM': True,
        'model_name': 'NN_QPMS_good_ALL'
    }

    # params.update(CRNN_params)
    params.update(Residual_FC_params)

    AE_params = {
        'hidden_units': [64, 32, 16],
        'batch_norm': [False, False, False],
        'dropout_keep_prob': [0.0, 0.0, 0.0],

        'activation': tf.nn.sigmoid,

        'learning_rate': 0.001,
        'saved_path': './',
        'restore': False,

        'batch_size': 128,
        'epochs': 100,
        'height': 1,  # windowing_size
        'width': 54,  # features
        'noise_ratio': 0.20,
    }
    from DAE import DAE
    from Residual_FC_model import Residual_FC_model

    model_name = 'testing'
    params['width'] = AE_params['width'] = 30
    data = np.random.normal(size=(3000, 30))
    activate = activator(Residual_FC_model, params, model_name, DAE, AE_params)
    sess = tf.Session()
    activate.build_net(sess)