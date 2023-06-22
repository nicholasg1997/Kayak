from MultiLabelPredictor import MultilabelPredictor
import pandas as pd
from autogluon.tabular import TabularDataset

degree_days = 'gw_hdd'

multi_predictor = MultilabelPredictor.load(f"models/{degree_days}")


