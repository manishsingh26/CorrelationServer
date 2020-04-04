# Author: Manish Singh


import os
import sys
import ast
import glob
from zipfile import ZipFile
from PythonScript.correlation_operation import create_dict, get_all_corr


class CorrelationMain(object):

    def __init__(self, pattern_data, corr_col_index1, corr_col_index2, total_file, corr_data):

        try:

            self.root_path = r"C:\Users\m4singh\PycharmProjects\CorrelationServer\UploadDirectory\\"

            self.index_timestamp = pattern_data.split("_")[-1].replace(".csv", "")
            self.file_path = pattern_data   #self.root_path + os.path.sep + pattern_data
            self.corr_col_index1 = ast.literal_eval(corr_col_index1)
            self.corr_col_index2 = ast.literal_eval(corr_col_index2)
            self.total_file = total_file

            self.feature_folder_path = self.root_path
            self.corr_output_path = self.feature_folder_path + corr_data.replace(".csv", self.index_timestamp + ".csv")

        except Exception as error1:
            print("[Error] [Pattern Correlation] [Init] :: ", error1)

    def correlation_executor(self):
        try:
            create_dict(self.file_path, self.corr_output_path, self.corr_col_index1, self.corr_col_index2, self.total_file)
            get_all_corr(file_name=self.corr_output_path, dest_folder=self.feature_folder_path, index_time=self.index_timestamp)
            zip_fle_name = "correlation_data_" + self.index_timestamp + ".zip"
            zip_file_path = self.feature_folder_path + os.path.sep + zip_fle_name

            with ZipFile(zip_file_path, "w") as zip_file:
                for files in glob.glob(self.feature_folder_path + os.path.sep + "*.csv"):
                    if files.endswith(self.index_timestamp + ".csv") and ("correlations" in files or "golden" in files
                                                                          or "corr_data" in files):
                        zip_file.write(files)
                        os.remove(files)
            return zip_file_path

        except Exception as error2:
            print("[Error] [Pattern Correlation] [Main] :: ", error2)
