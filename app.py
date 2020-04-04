# Author: Manish Singh


from flask import Flask


app = Flask(__name__)
app.secret_key = 'secret key'
app.config['upload_folder'] = r'C:\Users\m4singh\PycharmProjects\CorrelationServer\UploadDirectory'
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024
app.config['script_directory'] = r'C:\Users\m4singh\PycharmProjects\CorrelationServer\PythonScript'
app.config['corr_data'] = "corr_data_.csv"
app.config['root_path'] = r"C:\Users\m4singh\PycharmProjects\CorrelationServer\UploadDirectory\\"
