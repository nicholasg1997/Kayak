import os.path

import pandas as pd
from autogluon.tabular import TabularDataset

from MultiLabelPredictor import MultilabelPredictor
from process_raw_data import ProcessRawData

degree_days = 'ew_cdd'

if not os.path.exists(f'master_df_{degree_days}.pkl'):
    print(f'master_df_{degree_days}.pkl not found, creating it')
    ProcessRawData(degree_days=degree_days)
else:
    print(f'master_df_{degree_days}.pkl found, loading it')


master_df = pd.read_pickle(f'master_df_{degree_days}.pkl')

train_len = 0.85
train_data = TabularDataset(master_df[:int(len(master_df) * train_len)])
test_data = TabularDataset(master_df[int(len(master_df) * train_len):])

labels = ['ecmwf-eps_9', 'ecmwf-eps_10', 'ecmwf-eps_11', 'ecmwf-eps_12', 'ecmwf-eps_13',
          'ecmwf-eps_14']


save_path = f'models/{degree_days}'

if not os.path.exists(save_path):
    # Create the directory
    os.makedirs(save_path)

multi_predictor = MultilabelPredictor(labels=labels, path=save_path)
multi_predictor.fit(train_data, presets='best_quality') #presets = ['best_quality', 'optimize_for_deployment']

# Predict on test data
test_data_nolab = test_data.drop(columns=labels)
test_data_nolab.head()

evaluations = multi_predictor.evaluate(test_data)

print("evaluations")
print(evaluations)
