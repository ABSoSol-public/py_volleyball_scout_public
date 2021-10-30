"""DV-Reader
    * reader for data-volley files
    * is able to struct a data-volley file to a dictionary
    
    author: Alfred Becker
    mail: info@absosol.de
    date: 30.10.2021
    license: MIT (free for everything)

"""
import pandas as pd



class Data_Volley_Parser():
    """Data Volley Parser
        class for readout data volley files to get the structure and handle data to a new protocol
    """
    __keyword_list = []
    __col_name = "data_volley_data"

    # structure of data volley files (max parsed)
    __dv_datavolleyscout      = {"start_id":None , "end_id":None, "data":None}
    __dv_match                = {"start_id":None , "end_id":None, "data":None}
    __dv_teams                = {"start_id":None , "end_id":None, "data":None}
    __dv_more                 = {"start_id":None , "end_id":None, "data":None}
    __dv_comments             = {"start_id":None , "end_id":None, "data":None}
    __dv_set                  = {"start_id":None , "end_id":None, "data":None}
    __dv_players_h            = {"start_id":None , "end_id":None, "data":None}
    __dv_players_v            = {"start_id":None , "end_id":None, "data":None}
    __dv_attackcombination    = {"start_id":None , "end_id":None, "data":None}
    __dv_settercall           = {"start_id":None , "end_id":None, "data":None}
    __dv_winningsymbols       = {"start_id":None , "end_id":None, "data":None}
    __dv_reserve              = {"start_id":None , "end_id":None, "data":None}
    __dv_scout                = {"start_id":None , "end_id":None, "data":None}

    __dv_protocol = { "[3DATAVOLLEYSCOUT]"    : __dv_datavolleyscout, 
                    "[3MATCH]"              : __dv_match,
                    "[3TEAMS]"              : __dv_teams,
                    "[3MORE]"               : __dv_more,
                    "[3COMMENTS]"           : __dv_comments,
                    "[3SET]"                : __dv_set,
                    "[3PLAYERS-H]"          : __dv_players_h,
                    "[3PLAYERS-V]"          : __dv_players_v,
                    "[3ATTACKCOMBINATION]"  : __dv_attackcombination,
                    "[3SETTERCALL]"         : __dv_settercall,
                    "[3WINNINGSYMBOLS]"     : __dv_winningsymbols,
                    "[3RESERVE]"            : __dv_reserve,
                    "[3SCOUT]"              : __dv_scout}




    def create_raw_pd_file(self):
        pass




    def get_keyword_list(self):
        return self.__keyword_list




    def read_dv_file(self, file_path):
        """read_dv_file
            * reads out data volley file
            * bring file to a structured dictionary
            * for 1 - prepare all start ids from raw data
            * for 2 - set all end_id depend on start_id
            * for 3 - store all data lines to a data list to specific key
            * create a new public variable: "file_struct"
        Args:
            file_path (glob path): path to your data volley file
        """
        file_linedata = open(file_path,"r", encoding="Latin-1").readlines()
        pd_file = pd.DataFrame(file_linedata, columns=["filedata"])
        pd_file["filedata"] = pd_file["filedata"].str.replace("\n","")
        keylist_in_file = pd_file[pd_file["filedata"].str.startswith("[")]
        self.file_struct = self.__dv_protocol

        # set start_ids where we find the keyword inside the file
        for entry in keylist_in_file["filedata"]:
            self.file_struct[entry]["start_id"] = keylist_in_file[keylist_in_file["filedata"] == entry].index[0]
        
        # prepare file IDs
        last_keyname = None
        key_counter = 0
        for key in self.file_struct:
            key_counter += 1
            if last_keyname != None:
                self.file_struct[last_keyname]["end_id"] = self.file_struct[key]["start_id"] - 1
            if key_counter == len(self.file_struct):
                self.file_struct[key]["end_id"] = len(pd_file) - 1
            last_keyname = key

        # collect data lines
        for key in self.file_struct:
            if self.file_struct[key]["start_id"] == self.file_struct[key]["end_id"]:
                self.file_struct[key]["data"] = []
            else:
                self.file_struct[key]["data"] = list(pd_file.loc[self.file_struct[key]["start_id"]+1:self.file_struct[key]["end_id"]]["filedata"]) 




    def __check_keyword_list(self, new_keywords=[]):
        """check_keyword_list
            * private function
            * generate a keyword list over all your datavolley files to check if there is sth new
            * u are able to check the keys with the given protocol
        Args:
            new_keywords (list, optional): [description]. Defaults to [].
        """
        set_list = list(set(new_keywords[self.__col_name]))
        self.__keyword_list = self.__keyword_list + set_list
        self.__keyword_list = list(set(self.__keyword_list))




    def check_keywords_in_data_volleyscout(self, file):
        """check keywords in dv-file
            * readout dv-file and check which keywords are inside the file
            * all keywords are necessary for a correct work inside data volley application

        Args:
            file (glob path): path to a dv-file
        """
        check_file = file
        try:
            file_linedata = open(check_file,"r", encoding="Latin-1").readlines()
            pd_linedata = pd.DataFrame(file_linedata, columns=[self.__col_name])
            pd_linedata[self.__col_name] = pd_linedata[self.__col_name].str.replace("\n","")
            keywords_in_scout = pd_linedata[pd_linedata[self.__col_name].str.startswith("[")]
            self.__check_keyword_list(new_keywords=keywords_in_scout)
        except Exception as reader_error:
            print(str(reader_error))