from pathlib import Path
import pandas as pd
import uuid 
# Game Info: ['2024', 'PARIS', 'M80', '1_16', '303', 'CHI', 'CHURCHILL', 'MARTINEZ']
ATHLETE_ID_COLUMN = ['athlete_id', 'name', 'country']
MATCH_ID_COLUMN = ['match_id', 'tournament', 'weight_class', 'round', 'number']
TRANSFORMED_COLUMNS_SET = ['set_performance_id','athlete_id', 'match_id', 'set_number']
TRANSFORMED_COLUMNS_MATCH = ['match_performance_id','athlete_id', 'match_id']
TRANSFORMED_RESULT = ['SO','MO','SPP','SOP','SPF','SOF']# 單局勝負結果,整場勝負結果,單局選手得分,單局對手得分,單局選手犯規次數,單局對手犯規次數
TRANSFORMED_SKILL_MAP = {'拳':'M_pun','推':'cut','橫':'rhk','下劈':'axk','內擺':'H_ins','勾':'hok','雙':'jtk','反橫':'rrh','後踢':'bak','後旋':'bhk','旋風':'tor','旋下':'sax','旋內擺':'H_sin'}
# '下劈' is a special case, if '下劈' appear, HIT must be '高' or 'H', means hit head
# '旋下劈' = '旋下高' -> 'H_sax', '旋下劈' = '旋下中' -> 'M_sax'
TRANSFORMED_SKILL_SIDE_MAP = {'左':'L','右':'R'}
TRANSFORMED_SKILL_HIT_MAP = {'中':'M','高':'H','劈':'H'}
TRANSFORMED_SKILL_METRICS_MAP = {'使用次數':'UT','使用率':'UR','得分次數':'ST','得分率':'SR','得分值':'SP','得分占比':'PR'}

TRANSFORMED_STRATEGY_MAP = {'進攻':'at','迎擊':'in','反擊':'co','防守':'de'}
TRANSFORMED_STRATEGY_METRICS_MAP = {'發起次數':'OT','得分次數':'ST','得分值':'SP','戰術中技術使用次數':'TUT','犯規次數':'FT'}

TRANSFORMED_RHYTHM_MAP = {'長回合':'LC','短回合':'SC','交手回合總計':'TC'}
TRANSFORMED_RHYTHM_METRICS_MAP = {'回合數':'N','技術使用次數':'TET','單一技術次數':'ST','組合技術次數':'CT','戰術次數':'TAT','犯規次數':'FT'}
TRANSFORMED_RHYTHM_METRICS_MAP_2 = {'戰鬥時間':'FT','休息時間':'RT','戰鬥/休息比':'FRR'}

TRANSFORMED_POSITION_MAP = {'左開勢':'LO','右開勢':'RO','左閉勢':'LC','右閉勢':'RC'}
TRANSFORMED_POSITION_METRICS_MAP = {'啟動次數':'T','啟動佔比':'R'}

TRANSFORMED_FOUL_MAP = {'倒地':'FD','出界':'CB','暫停後攻擊':'AA','攻擊倒地選手':'AF','抓':'GR','推':'PU','違規擊打頭部':'HF','不良言行':'MC','Ｘ':'X','擊打下肢':'AB','提膝':'LL','消極':'DM'}
TRANSFORMED_FOUL_METRICS_MAP = {'次數':'T'}

NAMESPACE = uuid.NAMESPACE_DNS
class DataProcessor:
    FOLDER_PATH = Path(__file__).parent / "test_data"

    def __init__(self, folder_path: str = None, max_files: int = -1):
        self.folder_path = Path(folder_path) if folder_path else self.FOLDER_PATH
        self.max_files = max_files  # -1 to read all files
        self.init_columns()
        self.init_df()
        # print(self.df_athlete.columns)
        # print(self.df_match.columns)
        # print(self.df_set_performance.columns)
        # print(self.df_total_performance.columns)
        
    def init_columns(self)->None:
        # initialize the columns
        self.set_columns = TRANSFORMED_COLUMNS_SET.copy()
        self.set_columns.extend(TRANSFORMED_RESULT.copy())
        self.match_columns = TRANSFORMED_COLUMNS_MATCH.copy()
        self.match_columns.extend(TRANSFORMED_RESULT.copy())
        self.match_columns.remove('SO') # only have single match result, no need for single set result
        # skill columns (side_hit_skill)
        # print(f'cloumns len: {len(columns)}')
        for metric in TRANSFORMED_SKILL_METRICS_MAP.values():
            for side in TRANSFORMED_SKILL_SIDE_MAP.values():
                for skill in TRANSFORMED_SKILL_MAP.values():
                    if skill[0] in ['M','H']:# means no need to distinguish hit position
                        self.set_columns.append(f"{metric}_{side}_{skill}")
                        self.match_columns.append(f"{metric}_{side}_{skill}")
                    else:
                        for hit in ['M','H']:
                            self.set_columns.append(f"{metric}_{side}_{hit}_{skill}")
                            self.match_columns.append(f"{metric}_{side}_{hit}_{skill}")
                            
        # strategy columns
        # print(f'cloumns len: {len(columns)}')
        for metric in TRANSFORMED_STRATEGY_METRICS_MAP.values():
            for strategy in TRANSFORMED_STRATEGY_MAP.values():
                self.set_columns.append(f"{metric}_{strategy}")
                self.match_columns.append(f"{metric}_{strategy}")
                
        # rhythm columns
        # print(f'cloumns len: {len(columns)}')
        for metric in TRANSFORMED_RHYTHM_METRICS_MAP.values():
            for rhythm in TRANSFORMED_RHYTHM_MAP.values():
                self.set_columns.append(f"{metric}_{rhythm}")
                self.match_columns.append(f"{metric}_{rhythm}")
        for metric in TRANSFORMED_RHYTHM_METRICS_MAP_2.values():
            self.set_columns.append(f"{metric}")
            self.match_columns.append(f"{metric}")
                
        # position columns
        # print(f'cloumns len: {len(columns)}')
        for metric in TRANSFORMED_POSITION_METRICS_MAP.values():
            for position in TRANSFORMED_POSITION_MAP.values():
                self.set_columns.append(f"{metric}_{position}")
                self.match_columns.append(f"{metric}_{position}")
                
        # foul columns
        # print(f'cloumns len: {len(columns)}')
        for metric in TRANSFORMED_FOUL_METRICS_MAP.values():
            for foul in TRANSFORMED_FOUL_MAP.values():
                self.set_columns.append(f"{metric}_{foul}")
                self.match_columns.append(f"{metric}_{foul}")
                
        # print(f"Initialized columns: {columns}\nTotal columns: {len(columns)}")
        # Check for duplicate column names
        seen = set()
        duplicates = set()
        for c in self.set_columns:
            if c in seen:
                duplicates.add(c)
            else:
                seen.add(c)
        has_duplicates = len(self.set_columns) != len(set(self.set_columns))
        if has_duplicates:
            print("Warning: There are duplicate set column names in the initialized columns.")
            print(f"Duplicate columns: {duplicates}")
            
        seen = set()
        duplicates = set()
        for c in self.match_columns:
            if c in seen:
                duplicates.add(c)
            else:
                seen.add(c)
        has_duplicates = len(self.match_columns) != len(set(self.match_columns))
        if has_duplicates:
            print("Warning: There are duplicate match column names in the initialized columns.")
            print(f"Duplicate columns: {duplicates}")
        
      
    def init_df(self):
        self.df_athlete = pd.DataFrame(columns=ATHLETE_ID_COLUMN)
        self.df_match = pd.DataFrame(columns=MATCH_ID_COLUMN)
        self.df_set_performance = pd.DataFrame(columns=self.set_columns)
        self.df_match_performance = pd.DataFrame(columns=self.match_columns)  
        
    def readXlsx(self) -> pd.DataFrame:
        """Walk folder_path, call dataTransform on each xlsx file.
        Respects max_files: -1 means all files, otherwise stops after max_files files.
        Returns True on success, False if an error occurs.
        """
        try:
            count = 0
            for path in self.folder_path.rglob("*.xlsx"):
                if self.max_files != -1 and count >= self.max_files:
                    self.write_to_csv_xlsx()
                    return [self.df_athlete, self.df_match, self.df_set_performance, self.df_match_performance]
                if not path.name.startswith("~$"):
                    try:
                        count += 1
                        third_round = self.splitTable(path)
                        self.transform_data(third_round)
                        
                    except Exception as e:
                        print(f"Skipping {path}: {e}")
            self.write_to_csv_xlsx()
            return [self.df_athlete, self.df_match, self.df_set_performance, self.df_match_performance]
        except Exception as e:
            print(f"Error reading xlsx files: {e}")
            return []
    
    def write_to_csv_xlsx(self):
        """Write the transformed DataFrames to CSV files."""
        try:
            self.df_athlete.to_excel('test_data_results/athlete.xlsx', index=False)
            self.df_match.to_excel('test_data_results/match.xlsx', index=False)
            self.df_set_performance.to_excel('test_data_results/set_performance.xlsx', index=False)
            self.df_match_performance.to_excel('test_data_results/match_performance.xlsx', index=False)
            print("DataFrames successfully written to Excel files.")
        except Exception as e:
            print(f"Error writing DataFrames to Excel: {e}")
            raise
        
    def transform_data(self, third_round: bool) -> None:
        """Transform the extracted tables into the desired format."""
        # build athlete_id by country and name
        athlete_name = self.game_info[6]+'_'+self.game_info[7]
        athlete_country = self.game_info[5]
        athlete_id = str(uuid.uuid5(NAMESPACE, f"{athlete_country}_{athlete_name}"))
        new_athlete = pd.DataFrame([[athlete_id,athlete_name, athlete_country]], columns=ATHLETE_ID_COLUMN)
        if self.df_athlete.empty:
            self.df_athlete = new_athlete
        elif athlete_id not in self.df_athlete['athlete_id'].values:
            self.df_athlete = pd.concat([self.df_athlete, new_athlete], ignore_index=True)
        # print(self.df_athlete)
           
        # build match_id by tournament, weight_class, round, number
        match_tournament = self.game_info[0]+'_'+self.game_info[1]
        match_weight_class = self.game_info[2]
        match_round = self.game_info[3]
        match_number = self.game_info[4]
        match_id = str(uuid.uuid5(NAMESPACE, f"{match_tournament}_{match_weight_class}_{match_round}_{match_number}"))      
        new_match = pd.DataFrame([[match_id, match_tournament, match_weight_class, match_round, match_number]], columns=MATCH_ID_COLUMN)
        if self.df_match.empty:
            self.df_match = new_match
        elif match_id not in self.df_match['match_id'].values:
            self.df_match = pd.concat([self.df_match, new_match], ignore_index=True)
        # print(self.df_match)
        
        # build match_performance_id by athlete_id and match_id
        match_performance_id = str(uuid.uuid5(NAMESPACE, f"{athlete_id}_{match_id}"))
        
        # build set_performance_id by athlete_id, match_id and set_number
        set_performance_id = {
            'round1': str(uuid.uuid5(NAMESPACE, f"{athlete_id}_{match_id}_1")),
            'round2': str(uuid.uuid5(NAMESPACE, f"{athlete_id}_{match_id}_2")),
            'round3': str(uuid.uuid5(NAMESPACE, f"{athlete_id}_{match_id}_3"))
        }
        
        # basic information variables
        # Game Info: ['2024', 'PARIS', 'M80', '1_16', '303', 'CHI', 'CHURCHILL', 'MARTINEZ']
        # ['set_performance_id','athlete_id', 'match_id', 'set_number']
        # ['match_performance_id','athlete_id', 'match_id']
        
        temp_df = {
                   'round1':dict(),
                   'round2':dict(),
                   'round3':dict(),
                   'total':dict()}
        temp_df['total']['match_performance_id'] = match_performance_id
        temp_df['total']['athlete_id'] = athlete_id
        temp_df['total']['match_id'] = match_id
        
        for round_key in ['round1', 'round2', 'round3']:
            if not third_round and round_key == 'round3':
                continue
            temp_df[round_key]['set_performance_id'] = set_performance_id[round_key]
            temp_df[round_key]['athlete_id'] = athlete_id
            temp_df[round_key]['match_id'] = match_id
            temp_df[round_key]['set_number'] = round_key[-1]
            
        # print(f"Temp DF after basic info:\n{temp_df}\n")
        
        # game result variables
        # print(f"Game Result:\n{self.game_result}\n")
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            temp_df[round_key]['MO'] = 1 if self.game_result['total'].loc[0, '勝負結果'] == '勝' else 0
            temp_df[round_key]['SPP'] = self.game_result[round_key].loc[0, '選手得分']
            temp_df[round_key]['SOP'] = self.game_result[round_key].loc[0, '對手得分']
            temp_df[round_key]['SPF'] = self.game_result[round_key].loc[0, '選手犯規次數']
            temp_df[round_key]['SOF'] = self.game_result[round_key].loc[0, '對手犯規次數']
            if round_key != 'total':
                temp_df[round_key]['SO'] = 1 if self.game_result[round_key].loc[0, '勝負結果'] == '勝' else 0
            
        # skill variables
        # print(f"Skill:\n{self.skill}\n")
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            for skill_row in self.skill[round_key].itertuples(index=False):
                for metric_key in skill_row._fields[1:]:
                    metric = TRANSFORMED_SKILL_METRICS_MAP[metric_key]
                    side = TRANSFORMED_SKILL_SIDE_MAP[skill_row.技術名稱[0]]
                    hit_key = skill_row.技術名稱[-1]
                    
                    if hit_key in ['拳','擺']:
                        skill = TRANSFORMED_SKILL_MAP[skill_row.技術名稱[1:]]
                        dict_key = f'{metric}_{side}_{skill}'
                    else: 
                        if skill_row.技術名稱[1:] == '下劈':
                            skill = TRANSFORMED_SKILL_MAP[skill_row.技術名稱[1:]]# means '下劈' case, hit_key is '劈', skill name is '下劈'(the only overlapped case)
                        else:
                            skill = TRANSFORMED_SKILL_MAP[skill_row.技術名稱[1:-1]]
                        hit = TRANSFORMED_SKILL_HIT_MAP[hit_key]
                        dict_key = f'{metric}_{side}_{hit}_{skill}'      
                    temp_df[round_key][dict_key] = getattr(skill_row, metric_key)
        
        # strategy variables
        # print(f"Strategy:\n{self.strategy}\n")
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            for strategy_row in self.strategy[round_key].itertuples(index=False):
                for metric_key in strategy_row._fields[1:]:
                    metric = TRANSFORMED_STRATEGY_METRICS_MAP[metric_key]
                    strategy = TRANSFORMED_STRATEGY_MAP[strategy_row.戰術名稱]
                    dict_key = f'{metric}_{strategy}'
                    temp_df[round_key][dict_key] = getattr(strategy_row, metric_key)
        # print(f"Temp DF after strategy:\n{temp_df}\n")
        
        # rhythm variables
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            for rhythm_row in self.match_time[round_key].itertuples(index=False):
                for metric_key in rhythm_row._fields[1:]:
                    metric = TRANSFORMED_RHYTHM_METRICS_MAP[metric_key]
                    rhythm = TRANSFORMED_RHYTHM_MAP[rhythm_row.回合類型]
                    dict_key = f'{metric}_{rhythm}'
                    temp_df[round_key][dict_key] = getattr(rhythm_row, metric_key)
            temp_df[round_key]['FT'] = self.game_result[round_key].loc[0, '戰鬥時間']
            temp_df[round_key]['RT'] = self.game_result[round_key].loc[0, '休息時間']
            temp_df[round_key]['FRR'] = self.game_result[round_key].loc[0, '戰鬥/休息比']
        
        
        # position variables
        # print(f"Position:\n{self.position}\n")
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            for position_row in self.position[round_key].itertuples(index=False):
                for position_key in position_row._fields[1:]:
                    metric = TRANSFORMED_POSITION_METRICS_MAP[position_row.站架類型]
                    position = TRANSFORMED_POSITION_MAP[position_key]
                    dict_key = f'{metric}_{position}'
                    temp_df[round_key][dict_key] = getattr(position_row, position_key)
        # print(f"Temp DF after position:\n{temp_df}\n")
                    
        # foul variables
        # print(f"Foul:\n{self.foul}\n")     
        for round_key in ['round1', 'round2', 'round3', 'total']:
            if not third_round and round_key == 'round3':
                continue
            for foul_row in self.foul[round_key].itertuples(index=False):
                for foul_key in foul_row._fields[1:]:
                    metric = 'T' # only have '次數' metric for foul
                    foul = TRANSFORMED_FOUL_MAP[foul_key]
                    dict_key = f'{metric}_{foul}'
                    temp_df[round_key][dict_key] = getattr(foul_row, foul_key)
        # print(f"Temp DF after foul:\n{temp_df}\n")
        # print(f'length = {len(temp_df["round1"].keys())}')
        
        self.df_match_performance.loc[len(self.df_match_performance)] = temp_df['total']
        for round_key in ['round1', 'round2', 'round3']:
            if not third_round and round_key == 'round3':
                continue
            self.df_set_performance.loc[len(self.df_set_performance)] = temp_df[round_key]
        
    
    def splitTable(self, file: str) -> bool:
        """Read all sheets of the xlsx file, split tables, and return a DataFrame."""
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.unicode.ambiguous_as_wide', True)
        pd.set_option('display.unicode.east_asian_width', True)
        try:
            # print(f"Processing file: {file}")
            df = pd.read_excel(file, sheet_name=None, header=None)
            # df_game_result = df['比賽總計']
            # df_skill = df['技術統計']
            # df_stradgy = df['戰術統計']
            # df_match_time = df['交手回合時間與戰術統計']
            # df_position = df['交手回合站架統計']
            # df_foul = df['犯規統計']
            
            # game result table
            self.game_info = df['比賽總計'].iloc[0,0].split(' ')
            # print(f"Game Info: {self.game_info}")
            self.game_result = {}
            START = 1
            COLUMN = df['比賽總計'].iloc[START,:]
            START += 1
            THIRD_ROUND = True
            for round_key in ['round1', 'round2', 'round3', 'total']:
                self.game_result[round_key] = df['比賽總計'].iloc[START:START+1,:]
                self.game_result[round_key].columns = COLUMN
                self.game_result[round_key].columns.name = None
                self.game_result[round_key] = self.game_result[round_key].reset_index(drop=True)
                START += 1
                # print(f"Game Result {round_key}:\n{self.game_result[round_key]}\n")
            if self.game_result['round3'].loc[0, '勝負結果'] == '-':
                THIRD_ROUND = False
            # skill table
            SHIFT = 24
            START = 2
            self.skill = {}
            for round_key in ['round1', 'round2', 'round3', 'total']:
                temp = df['技術統計'].iloc[START:START+SHIFT,:7]
                temp.columns = temp.iloc[0]
                temp = temp.drop(temp.index[0])
                temp2 = df['技術統計'].iloc[START:START+SHIFT,7:]
                temp2.columns = temp2.iloc[0]
                temp2 = temp2.drop(temp2.index[0])
                self.skill[round_key] = pd.concat([temp, temp2]).reset_index(drop=True)
                self.skill[round_key].columns.name = None
                START += SHIFT +1
                # print(f"Skill {round_key}:\n{self.skill[round_key]}")
            
            # strategy table
            SHIFT = 5
            START = 2
            self.strategy = {}
            for round_key in ['round1', 'round2', 'round3', 'total']:
                temp = df['戰術統計'].iloc[START:START+SHIFT,:]
                temp.columns = temp.iloc[0]
                temp = temp.drop(temp.index[0])
                self.strategy[round_key] = temp.reset_index(drop=True)
                self.strategy[round_key].columns.name = None
                START += SHIFT +1
                # print(f"Strategy {round_key}:\n{self.strategy[round_key]}\n")
                
            # match time table
            SHIFT = 4
            START = 2
            self.match_time = {}
            for round_key in ['round1', 'round2', 'round3', 'total']:
                temp = df['交手回合時間與戰術統計'].iloc[START:START+SHIFT,:]
                temp.columns = temp.iloc[0]
                temp = temp.drop(temp.index[0])
                self.match_time[round_key] = temp.reset_index(drop=True)
                self.match_time[round_key].columns.name = None
                START += SHIFT +1
                # print(f"Match Time {round_key}:\n{self.match_time[round_key]}\n")
                
            # position table
            SHIFT = 3
            START = 2
            self.position = {}
            for round_key in ['round1', 'round2', 'round3', 'total']:
                temp = df['交手回合站架統計'].iloc[START:START+SHIFT,:]
                temp.columns = temp.iloc[0]
                temp = temp.drop(temp.index[0])
                self.position[round_key] = temp.reset_index(drop=True)
                self.position[round_key].columns.name = None
                START += SHIFT +1
                # print(f"Position {round_key}:\n{self.position[round_key]}\n")
                
            # foul table
            SHIFT = 2
            START = 2
            self.foul = {}
            for round_key in ['round1', 'round2', 'round3', 'total']:
                temp = df['犯規統計'].iloc[START:START+SHIFT,:]
                temp.columns = temp.iloc[0]
                temp = temp.drop(temp.index[0])
                self.foul[round_key] = temp.reset_index(drop=True)
                self.foul[round_key].columns.name = None
                START += SHIFT +1
                # print(f"Foul {round_key}:\n{self.foul[round_key]}\n")
            return THIRD_ROUND

        except Exception as e:
            print(f"Error transforming data from {file}: {e}")
            raise

    def outputTest(self, file: str) -> None:
        """Print the full content of all sheets of an xlsx file."""
        xl = pd.read_excel(file, sheet_name=None, header=None)
        for sheet_name, df in xl.items():
            print(f"=== Sheet: {sheet_name} ===")
            print(df)
        
if __name__ == "__main__":
    processor = DataProcessor(max_files=1)
    df = processor.readXlsx()
    
    # for name, dataframe in zip(['athlete', 'match', 'set_performance', 'match_performance'], df):
    #     print(f"DataFrame: {name}")
    #     print(dataframe)
    # success = processor.readXlsx()
    # if success:
    #     print("All files processed successfully.")
    # else:
    #     print("Some files could not be processed.")

        