
NN_params_basic = {
	'l1_beta': 0.00,
	'l2_beta': 0.0001,
	'learning_rate': 0.001,
	'keep_probs': [0.5, 0.5, 0.5],
	'ffn_dims': [64, 32, 16],
	'activation': 'relu',
	'radam': True # RADAM 사용하면 True
}

NN_train_params_basic = {
	'epochs': 50,
	'minimum_epoch': 10,
	'monitor': 'val_loss',
	'batch_size': 32, 
	'verbose': True,
	'early_stopping': 10,
	'pre_train': True,
}


LGBM_params_basic = {
	'max_depth': 10,
	'num_leaves': 50,
	'sub_sample': 0.8,
	'feature_fraction': 0.8,
	'learning_rate': 0.001,
	'n_estimator': 1000,
}

LGBM_train_params_basic = {
	'early_stopping_rounds': 50,
	'verbose': True,
}