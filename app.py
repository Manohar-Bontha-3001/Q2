from flask import Flask, request, render_template, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        mag = request.form.get('mag')
        date_range = request.form.get('date_range')
        lat = request.form.get('latitude')
        lon = request.form.get('longitude')
        distance = request.form.get('distance')

        if mag:
            earthquakes = Earthquake.query.filter(Earthquake.mag >= float(mag)).all()
            return render_template('results.html', earthquakes=earthquakes)

        if date_range:
            start_date, end_date = date_range.split(' - ')
            earthquakes = Earthquake.query.filter(Earthquake.time >= start_date, Earthquake.time <= end_date).all()
            return render_template('results.html', earthquakes=earthquakes)

        if lat and lon and distance:
            lat, lon, distance = float(lat), float(lon), float(distance)
            earthquakes = Earthquake.query.filter(
                func.sqrt(func.pow(Earthquake.latitude - lat, 2) + func.pow(Earthquake.longitude - lon, 2)) <= distance
            ).all()
            return render_template('results.html', earthquakes=earthquakes)

        # Additional queries can be handled here

    return render_template('query.html')

@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    count = Earthquake.query.filter(Earthquake.mag > 5.0).count()
    return f'Total earthquakes with magnitude greater than 5.0: {count}'

@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    night_earthquakes = Earthquake.query.filter(Earthquake.mag > 4.0, func.hour(Earthquake.time).between(18, 6)).count()
    return f'Total large earthquakes (>4.0 mag) at night: {night_earthquakes}'

if __name__ == '__main__':
    app.run(debug=True)
