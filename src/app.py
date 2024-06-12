import os
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generates a random secret key

# Connection string
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        data = pd.read_csv(file)
        with engine.connect() as connection:
            for index, row in data.iterrows():
                connection.execute(text('''
                    INSERT INTO Earthquake (
                        time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type
                    ) VALUES (:time, :latitude, :longitude, :depth, :mag, :magType, :nst, :gap, :dmin, :rms, :net, :id_earthquake, :updated, :place, :type)
                '''), {
                    'time': row['time'],
                    'latitude': row['latitude'],
                    'longitude': row['longitude'],
                    'depth': row['depth'],
                    'mag': row['mag'],
                    'magType': row['magType'],
                    'nst': row['nst'],
                    'gap': row['gap'],
                    'dmin': row['dmin'],
                    'rms': row['rms'],
                    'net': row['net'],
                    'id_earthquake': row['id'],
                    'updated': row['updated'],
                    'place': row['place'],
                    'type': row['type']
                })
        return redirect(url_for('index'))
    return 'No file uploaded', 400

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        mag = request.form.get('mag')
        date_range = request.form.get('date_range')
        lat = request.form.get('latitude')
        lon = request.form.get('longitude')
        distance = request.form.get('distance')

        with engine.connect() as connection:
            if mag:
                result = connection.execute(text('SELECT * FROM Earthquake WHERE mag >= :mag'), {'mag': mag})
                earthquakes = result.fetchall()
                return render_template('results.html', earthquakes=earthquakes)

            if date_range:
                start_date, end_date = date_range.split(' - ')
                result = connection.execute(text('SELECT * FROM Earthquake WHERE time BETWEEN :start_date AND :end_date'), {'start_date': start_date, 'end_date': end_date})
                earthquakes = result.fetchall()
                return render_template('results.html', earthquakes=earthquakes)

            if lat and lon and distance:
                result = connection.execute(text('''
                    SELECT *, 
                    (111.045 * DEGREES(ACOS(COS(RADIANS(:lat)) 
                    * COS(RADIANS(latitude)) 
                    * COS(RADIANS(:lon) - RADIANS(longitude)) 
                    + SIN(RADIANS(:lat)) 
                    * SIN(RADIANS(latitude))))) AS distance
                    FROM Earthquake
                    HAVING distance <= :distance
                '''), {'lat': float(lat), 'lon': float(lon), 'distance': float(distance)})
                earthquakes = result.fetchall()
                return render_template('results.html', earthquakes=earthquakes)

    return render_template('query.html')

@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    with engine.connect() as connection:
        result = connection.execute(text('SELECT COUNT(*) AS count FROM Earthquake WHERE mag > 5.0'))
        count = result.fetchone()[0]  # Accessing the first column directly
    return f'Total earthquakes with magnitude greater than 5.0: {count}'

@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    with engine.connect() as connection:
        result = connection.execute(text('''
            SELECT COUNT(*) AS count FROM Earthquake 
            WHERE mag > 4.0 
            AND (CAST(time AS TIME) >= '18:00:00' OR CAST(time AS TIME) <= '06:00:00')
        '''))
        count = result.fetchone()[0]  # Accessing the first column directly
    return f'Total large earthquakes (>4.0 mag) at night: {count}'

if __name__ == '__main__':
    app.run(debug=True)
