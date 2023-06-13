import pandas as pd
from autogluon.tabular import TabularDataset
from MultiLabelPredictor import MultilabelPredictor
import os.path

master_df = pd.read_pickle('master_df.pkl')

imp_features = list(
    {"day_8_error", "gfs-ens-bc_9", "ecmwf_diff_8", "noon", "cmc-ens_9", "gfs-ens-bc_10", "gfs-ens-bc_9",
     "ecmwf_diff_9", "cmc-ens_10", "noon", "gfs-ens-bc_10", "gfs-ens-bc_11", "gfs-ens-bc_9", "cmc-ens_11",
     "gfs-ens-bc_11", "gfs-ens-bc_12", "gfs-ens-bc_10", "gfs-ens-bc_12", "gfs-ens-bc_13"})

labels = ['ecmwf-eps_9', 'ecmwf-eps_10', 'ecmwf-eps_11', 'ecmwf-eps_12', 'ecmwf-eps_13',
          'ecmwf-eps_14']

train_len = 0.8
train_data = TabularDataset(master_df[:int(len(master_df)*train_len)])
test_data = TabularDataset(master_df[int(len(master_df)*train_len):])

save_path = 'models'

if not os.path.exists(save_path):
    # Create the directory
    os.makedirs(save_path)

multi_predictor = MultilabelPredictor(labels=labels, path=save_path)
multi_predictor.fit(train_data, presets='best_quality')





