# Author: Manish Singh


import os
import time
from app import app
from gevent.pywsgi import WSGIServer
from werkzeug.utils import secure_filename
from flask import flash, request, redirect, render_template
from flask import send_from_directory
from PythonScript.pattern_correlation_main import CorrelationMain
ALLOWED_EXTENSIONS = ['txt', 'csv']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/downloads/<filename>', methods=['GET', 'POST'])
def download(filename):
    uploads = r"C:\Users\m4singh\PycharmProjects\CorrelationServer\UploadDirectory"
    return send_from_directory(directory=uploads, filename=filename)


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/', methods=['POST'])
def upload_file():
    if request.method == 'POST':

        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']

        if file.filename == '':
            flash('No file selected for uploading')
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['upload_folder'], filename))

            flash('File successfully uploaded')

            left_col = "[" + str(request.form['left_column']) + "]"
            right_col = "[" + str(request.form['right_column']) + "]"
            file_sep = int(request.form['file_sep'])
            corr_data = app.config['corr_data']
            root_path = app.config['root_path']

            file_actual_name = root_path + filename
            file_new_name = file_actual_name.replace(".csv", "_" + str(int(time.time())) + ".csv")
            os.rename(file_actual_name, file_new_name)

            obj = CorrelationMain(file_new_name, left_col, right_col, file_sep, corr_data)
            zip_file_path = obj.correlation_executor()
            zip_name = zip_file_path.split(os.path.sep)[-1]

            flash('File Name : ' + str(zip_name))
            flash('Code Execution Completed')
            return redirect('/')

        else:
            flash('Allowed file types are txt, csv')
            return redirect(request.url)


if __name__ == "__main__":
    # app.run(host='0.0.0.0', debug=True)
    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()
