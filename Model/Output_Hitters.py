import sys
import sqlite3
import torch
from tqdm import tqdm
from Player_Prep import Init_Hitters, Generate_Hitters, Transform_Hitter
from Constants import h_fielding_components, h_hitting_components, h_init_components, h_park_components, h_person_components, h_stealing_components
from Model import RNN_Model, LSTM_Model
from Dataset import WAR_BUCKETS

def _Get_Signing_Age(birth_year, birth_month, birth_date, signing_year, signing_month, signing_date):
    return signing_year - birth_year + (signing_month - birth_month) / 12 + (signing_date / birth_date) / 365

model_name = sys.argv[1]

db = sqlite3.connect("../BaseballStats.db")
db.rollback()
db.create_function("signingAge", 6, _Get_Signing_Age)
cursor = db.cursor()

models = cursor.execute(f'''
                        SELECT ModelName, Year, HiddenSize, NumLayers, ModelIdx
                        FROM Model_TrainingHistory
                        WHERE ModelName LIKE '{model_name}%'
                        ''').fetchall()


model_idx = models[0][4]

# Delete any existing data
cursor.execute("DELETE FROM Output_PlayerWar WHERE ModelIdx=?", (model_idx,))
db.commit()
cursor = db.cursor()

# Initialize the model so that the scalar and pca are setup with the same values
Init_Hitters(models[0][1])
Generate_Hitters(h_fielding_components, h_hitting_components, h_stealing_components, h_park_components, h_person_components)

## Prepare Hitter Input
ids = cursor.execute('''
                            SELECT DISTINCT pcs.mlbId
                            FROM Player_CareerStatus AS pcs
                            INNER JOIN Player AS p ON pcs.mlbId = p.mlbId
                            WHERE isHitter IS NOT NULL
                            AND birthYear IS NOT NULL
                            AND careerStartYear>=?
                            ''', (2005,)).fetchall()

hitter_inputs = []
for id, in tqdm(ids, desc="Creating Hitter Input Data", leave=False):
    hitter_data = cursor.execute('''
                                        SELECT mlbId, 
                                        signingAge(birthYear, birthMonth, birthDate, signingYear, signingMonth, signingDate),
                                        draftPick
                                        FROM Player WHERE mlbId=?
                                        ''', (id,)).fetchone()
    hitter_input, _ = Transform_Hitter(hitter_data)
    hitter_inputs.append(hitter_input)
    
# Create Models
input_size = hitter_inputs[0].shape[1]
networks = []
for model, _, hidden_size, num_layers, _ in tqdm(models, desc="Creating Networks", leave=False):
    #network = RNN_Model(input_size, num_layers, hidden_size, 0, [])
    network = LSTM_Model(input_size, num_layers, hidden_size, 0, [])
    network.load_state_dict(torch.load('Models/' + model))
    network.eval()
    networks.append(network)

## Iterate each hitter through each model
cursor.execute("BEGIN TRANSACTION")
for i, input_data in tqdm(enumerate(hitter_inputs), desc="Evaluating Hitters on each model", leave=False, total=len(hitter_inputs)):
    input_length = input_data.shape[0]
    war_values = torch.zeros((len(networks), input_length, WAR_BUCKETS.shape[0]))
    for j, network in enumerate(networks):
        with torch.no_grad():
            output_war, _, _ = network(input_data.unsqueeze(0), torch.tensor(input_length).unsqueeze(0))
            output_war = output_war.squeeze(0)
            output_war = torch.nn.functional.softmax(output_war, dim=1)
            war_values[j,:,:] = output_war
    
    war_avgs = torch.mean(war_values, dim=0)
    #print(war_avgs.shape)
    #print(war_values.shape)
    #print(war_avgs)
    mlbId = ids[i][0]
    output_dates = [(0,0)] + cursor.execute("SELECT Year, Month FROM Model_HitterStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (mlbId,)).fetchall()
    #print(len(output_dates))
    
    for n, (year, month) in enumerate(output_dates):
        probs = war_avgs[n,:].tolist()
        
        cursor.execute('''
                       INSERT INTO Output_PlayerWar
                       VALUES(?,?,?,?,ROUND(?,3),ROUND(?,3),ROUND(?,3),ROUND(?,3),ROUND(?,3),ROUND(?,3),ROUND(?,3))
                       ''', tuple([mlbId, model_idx, year, month] + probs))
    
cursor.execute("END TRANSACTION")
db.commit()