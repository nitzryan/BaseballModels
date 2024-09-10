from Constants import db
from typing import List
import torch.nn as nn
import torch
from tqdm import tqdm
from Player_Prep import Transform_Hitter

def Delete_Model_Run_Hitter(model : str) -> None :
    db.rollback()
    cursor = db.cursor()
    ids = cursor.execute("SELECT outputId FROM Output_HitterResult WHERE modelVersion=?", (model,)).fetchall()
    cursor.execute("BEGIN TRANSACTION")
    cursor.executemany("DELETE FROM Output_HitterLevel WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_HitterPA WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_HitterWar WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_HitterResult WHERE outputId=?", ids)
    cursor.execute("END TRANSACTION")
    db.commit()

def _Get_Signing_Age(birth_year, birth_month, birth_date, signing_year, signing_month, signing_date):
    return signing_year - birth_year + (signing_month - birth_month) / 12 + (signing_date / birth_date) / 365
    
def Generate_Model_Run_Hitter(model_name : str, ids : List[int], model_train_ids : List[int], network : nn.Module, device : torch.device) -> None:
    inputs = []
    player_ids = []
    
    network.eval()
    db.rollback()
    db.create_function("signingAge", 6, _Get_Signing_Age)
    cursor = db.cursor()
    for id in tqdm(ids, desc="Read Player Data"):
        hitter_data = cursor.execute('''
                                        SELECT mlbId, 
                                        signingAge(birthYear, birthMonth, birthDate, signingYear, signingMonth, signingDate),
                                        draftPick
                                        FROM Player WHERE mlbId=? 
                                        ''', (id,)).fetchone()
    
        hitter_input, _ = Transform_Hitter(hitter_data)
        inputs.append(hitter_input)
        player_ids.append(id)
      
    cursor.execute("BEGIN TRANSACTION")
    for i in tqdm(range(len(inputs)), desc='Model Run'):
        padded_input = torch.nn.utils.rnn.pad_sequence(inputs[i]).unsqueeze(0)
        padded_input = padded_input.transpose(1,2)
        length = torch.tensor(inputs[i].size(0)).unsqueeze(0)
        padded_input = padded_input.to(device)
        
        with torch.no_grad():
            output_war, output_level, output_pa = network(padded_input, length)
        
        output_war = output_war.to("cpu").squeeze(0)
        output_level = output_level.to("cpu").squeeze(0)
        output_pa = output_pa.to("cpu").squeeze(0)
        
        output_war = torch.nn.functional.softmax(output_war, dim=1)
        output_level = torch.nn.functional.softmax(output_level, dim=1)
        output_pa = torch.nn.functional.softmax(output_pa, dim=1)
        
        id = player_ids[i]
        output_dates = [(0,0)] + cursor.execute("SELECT Year, Month FROM Model_HitterStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (id,)).fetchall()
        
        # Determine if player was in train/test data
        is_tainted = False
        for test_id in model_train_ids:
            if test_id == id:
                is_tainted = True
                break
        
        for j in range(output_war.size(0)):
            cursor.execute('''
                           INSERT INTO 
                           Output_HitterResult('isInitial','mlbId','year','month','modelVersion','isTainted')
                           VALUES (?,?,?,?,?,?)
                           ''', (j == 0, id, output_dates[j][0], output_dates[j][1], model_name, is_tainted))
            output_id = cursor.lastrowid
            cursor.execute("INSERT INTO Output_HitterWar VALUES(?,?,?,?,?,?,?,?)",
                           (output_id, output_war[j,0].item(),output_war[j,1].item(),output_war[j,2].item(),
                            output_war[j,3].item(),output_war[j,4].item(),output_war[j,5].item(),output_war[j,6].item()))
            cursor.execute("INSERT INTO Output_HitterLevel VALUES(?,?,?,?,?,?,?,?,?)",
                           (output_id, output_level[j,0].item(),output_level[j,1].item(),output_level[j,2].item(),
                            output_level[j,3].item(),output_level[j,4].item(),output_level[j,5].item(),output_level[j,6].item(),output_level[j,7].item()))
            cursor.execute("INSERT INTO Output_HitterPa VALUES(?,?,?,?,?,?,?)",
                           (output_id, output_pa[j,0].item(),output_pa[j,1].item(),output_pa[j,2].item(),
                            output_pa[j,3].item(),output_pa[j,4].item(),output_pa[j,5].item()))
        
    cursor.execute("END TRANSACTION")
    db.commit()