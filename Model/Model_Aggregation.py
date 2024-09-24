import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from tqdm import tqdm
import sys

from Model import RNN_Classification_Loss
from Constants import device, D_TYPE
from Dataset import WAR_BUCKETS

from Output import Delete_Model_Run_Hitter, Generate_Model_Run_Hitter
from Constants import db

# Get all models to aggregate
model_name = sys.argv[1]
agg_model = sys.argv[2]

db.rollback()
cursor = db.cursor()
cursor.execute("DELETE FROM Output_AggregateHitterWar WHERE model=?", (agg_model,))
db.commit()
cursor = db.cursor()

hitter_war = cursor.execute('''SELECT p.mlbId
                            FROM Model_Players AS p
                            WHERE p.isHitter = '1'
                            ''').fetchall()


models = cursor.execute(f"SELECT DISTINCT modelVersion FROM Output_HitterResult WHERE modelVersion LIKE '{model_name}%'").fetchall()

# Create aggregate predictions for each time step
aggregate_predictions = []
for id, in tqdm(hitter_war, desc="Creating Aggregate Predictions"):
    len_predictions = cursor.execute("SELECT COUNT(*) FROM Model_HitterStats WHERE mlbId=?", (id,)).fetchone()[0] + 1
    predictions = torch.zeros((len_predictions, 7), dtype=D_TYPE)
    for model, in models:
        outputIds = cursor.execute('''
                                   SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6
                                   FROM Output_HitterWAR AS w
                                   INNER JOIN Output_HitterResult as r on r.outputId = w.outputId
                                   WHERE r.mlbId=? AND r.modelVersion=? 
                                   ORDER BY w.outputId ASC
                                   ''', (id, model)).fetchall()
        for n, probs in enumerate(outputIds):
            probs = torch.tensor(probs, dtype=D_TYPE)
            predictions[n,:] += probs
    
    predictions /= len(models)
    
    dates = cursor.execute("SELECT DISTINCT month, year FROM Output_HitterResult WHERE mlbId=? ORDER BY year ASC, month ASC", (id,)).fetchall()
    
    for n in range(len(predictions)):
        aggregate_predictions.append((id,dates[n][0],dates[n][1],agg_model) + tuple(predictions[n,:].tolist()))
    
    # cursor.executemany("INSERT INTO Output_AggregateHitterWar VALUES(?,?,?,?,?,?,?,?,?,?,?)", aggregate_predictions)
    # db.commit()
    # cursor = db.cursor()
    # aggregate_predictions = []

cursor.execute("BEGIN TRANSACTION")
cursor.executemany("INSERT INTO Output_AggregateHitterWar VALUES(?,?,?,?,?,?,?,?,?,?,?)", aggregate_predictions)
cursor.execute("END TRANSACTION")
db.commit()