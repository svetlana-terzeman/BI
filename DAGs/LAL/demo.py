import warnings
warnings.filterwarnings("ignore")

import os
import sys
from pathlib                 import Path
project_root = Path(os.getcwd()).parent
sys.path.append(str(project_root))

import pandas as pd
from datetime           import datetime
from main               import start
from settings.constants import reserach_period
from libs.label_encoder      import MultiFeatureLabelEncoder #нужен для корректной десериализации MultiFeatureLabelEncoder, который используется в run_models.py

currdate =  pd.to_datetime(datetime.now()).date().strftime('%Y-%m-%d')
if __name__ == "__main__":
    print('Старт')
    start(reserach_period, currdate)

