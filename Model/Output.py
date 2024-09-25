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

    
def Setup_Players(ids: List[int], model_train_ids: List[int]) -> None :
    global _inputs
    global _player_ids
    global _model_train_ids
    
    _inputs = []
    _player_ids = []
    _model_train_ids = []
    
    db.rollback()
    db.create_function("signingAge", 6, _Get_Signing_Age)
    cursor = db.cursor()
    for (id,) in tqdm(ids, desc="Read Player Data", leave=False):
        hitter_data = cursor.execute('''
                                        SELECT mlbId, 
                                        signingAge(birthYear, birthMonth, birthDate, signingYear, signingMonth, signingDate),
                                        draftPick
                                        FROM Player WHERE mlbId=? 
                                        ''', (id,)).fetchone()
    
        hitter_input, _ = Transform_Hitter(hitter_data)
        _inputs.append(hitter_input)
        _player_ids.append(id)
    _model_train_ids = model_train_ids
    
def Generate_Model_Run_Hitter(model_name : str, network : nn.Module, device : torch.device, leave_progress: bool) -> None:
    global _inputs
    global _player_ids
    global _model_train_ids
    
    network.eval()
    db.rollback()
    cursor = db.cursor()
    
    cpu_network = network.to('cpu')
      
    cursor.execute("BEGIN TRANSACTION")
    for i in tqdm(range(len(_inputs)), desc='Model Run', leave=leave_progress):
        padded_input = torch.nn.utils.rnn.pad_sequence(_inputs[i]).unsqueeze(0)
        padded_input = padded_input.transpose(1,2)
        length = torch.tensor(_inputs[i].size(0)).unsqueeze(0)
        #padded_input = padded_input.to(device)
        
        with torch.no_grad():
            output_war, output_level, output_pa = cpu_network(padded_input, length)
        
        output_war = output_war.to("cpu").squeeze(0)
        output_level = output_level.to("cpu").squeeze(0)
        output_pa = output_pa.to("cpu").squeeze(0)
        
        output_war = torch.nn.functional.softmax(output_war, dim=1)
        output_level = torch.nn.functional.softmax(output_level, dim=1)
        output_pa = torch.nn.functional.softmax(output_pa, dim=1)
        
        id = _player_ids[i]
        output_dates = [(0,0)] + cursor.execute("SELECT Year, Month FROM Model_HitterStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (id,)).fetchall()
        
        # Determine if player was in train/test data
        is_tainted = False
        for test_id in _model_train_ids:
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
    
####### PITCHER VARIANTS ############

def Delete_Model_Run_Pitcher(model : str) -> None :
    db.rollback()
    cursor = db.cursor()
    ids = cursor.execute("SELECT outputId FROM Output_PitcherResult WHERE modelVersion=?", (model,)).fetchall()
    cursor.execute("BEGIN TRANSACTION")
    cursor.executemany("DELETE FROM Output_PitcherLevel WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_PitcherPA WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_PitcherWar WHERE outputId=?", ids)
    cursor.executemany("DELETE FROM Output_PitcherResult WHERE outputId=?", ids)
    cursor.execute("END TRANSACTION")
    db.commit()
    
def Generate_Model_Run_Pitcher(model_name : str, network : nn.Module, device : torch.device, leave_progress: bool) -> None:
    global _inputs
    global _player_ids
    global _model_train_ids
    
    _inputs = []
    _player_ids = []
    _model_train_ids = []
    
    network.eval()
    db.rollback()
    cursor = db.cursor()
    
    cpu_network = network.to('cpu')
      
    cursor.execute("BEGIN TRANSACTION")
    for i in tqdm(range(len(_inputs)), desc='Model Run', leave=leave_progress):
        padded_input = torch.nn.utils.rnn.pad_sequence(_inputs[i]).unsqueeze(0)
        padded_input = padded_input.transpose(1,2)
        length = torch.tensor(_inputs[i].size(0)).unsqueeze(0)
        #padded_input = padded_input.to(device)
        
        with torch.no_grad():
            output_war, output_level, output_bf = cpu_network(padded_input, length)
        
        output_war = output_war.to("cpu").squeeze(0)
        output_level = output_level.to("cpu").squeeze(0)
        output_bf = output_bf.to("cpu").squeeze(0)
        
        output_war = torch.nn.functional.softmax(output_war, dim=1)
        output_level = torch.nn.functional.softmax(output_level, dim=1)
        output_bf = torch.nn.functional.softmax(output_bf, dim=1)
        
        id = _player_ids[i]
        output_dates = [(0,0)] + cursor.execute("SELECT Year, Month FROM Model_PitcherStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (id,)).fetchall()
        
        # Determine if player was in train/test data
        is_tainted = False
        for test_id in _model_train_ids:
            if test_id == id:
                is_tainted = True
                break
        
        for j in range(output_war.size(0)):
            cursor.execute('''
                           INSERT INTO 
                           Output_PitcherResult('isInitial','mlbId','year','month','modelVersion','isTainted')
                           VALUES (?,?,?,?,?,?)
                           ''', (j == 0, id, output_dates[j][0], output_dates[j][1], model_name, is_tainted))
            output_id = cursor.lastrowid
            cursor.execute("INSERT INTO Output_PitcherWar VALUES(?,?,?,?,?,?,?,?)",
                           (output_id, output_war[j,0].item(),output_war[j,1].item(),output_war[j,2].item(),
                            output_war[j,3].item(),output_war[j,4].item(),output_war[j,5].item(),output_war[j,6].item()))
            cursor.execute("INSERT INTO Output_PitcherLevel VALUES(?,?,?,?,?,?,?,?,?)",
                           (output_id, output_level[j,0].item(),output_level[j,1].item(),output_level[j,2].item(),
                            output_level[j,3].item(),output_level[j,4].item(),output_level[j,5].item(),output_level[j,6].item(),output_level[j,7].item()))
            cursor.execute("INSERT INTO Output_PitcherPa VALUES(?,?,?,?,?,?,?)",
                           (output_id, output_bf[j,0].item(),output_bf[j,1].item(),output_bf[j,2].item(),
                            output_bf[j,3].item(),output_bf[j,4].item(),output_bf[j,5].item()))
        
    cursor.execute("END TRANSACTION")
    db.commit()

#####################################