from flask import Flask, request, render_template, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import pyodbc

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mssql+pyodbc://<username>:<password>@<server>/<database>?driver=ODBC+Driver+17+for+SQL+Server'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class Earthquake(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    time = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    depth = db.Column(db.Float)
    mag = db.Column(db.Float)
    magType = db.Column(db.String(10))
    nst = db.Column(db.Integer)
    gap = db.Column(db.Float)
    dmin = db.Column(db.Float)
    rms = db.Column(db.Float)
    net = db.Column(db.String(10))
    id_earthquake = db.Column(db.String(20))
    updated = db.Column(db.String(50))
    place = db.Column(db.String(100))
    type = db.Column(db.String(50))

db.create_all()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        data = pd.read_csv(file)
        for index, row in data.iterrows():
            earthquake = Earthquake(
                time=row['time'],
                latitude=row['latitude'],
                longitude=row['longitude'],
                depth=row['depth'],
                mag=row['mag'],
                magType=row['magType'],
                nst=row['nst'],
                gap=row['gap'],
                dmin=row['dmin'],
                rms=row['rms'],
                net=row['net'],
                id_earthquake=row['id'],
                updated=row['updated'],
                place=row['place'],
                type=row['type']
            )
            db.session.add(earthquake)
        db.session.commit()
        return redirect(url_for('index'))
    return 'No file uploaded', 400

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        mag = request.form['mag']
        earthquakes = Earthquake.query.filter(Earthquake.mag >= mag).all()
        return render_template('results.html', earthquakes=earthquakes)
    return render_template('query.html')

if __name__ == '__main__':
    app.run(debug=True)