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
                    INSERT INTO earthquakes (
                        time, latitude, longitude, depth, Magnitude, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type, local_time
                    ) VALUES (:time, :latitude, :longitude, :depth, :Magnitude, :magType, :nst, :gap, :dmin, :rms, :net, :id_earthquake, :updated, :place, :type, :local_time)
                '''), {
                    'time': row['time'],
                    'latitude': row['latitude'],
                    'longitude': row['longitude'],
                    'depth': row['depth'],
                    'Magnitude': row['mag'],
                    'magType': row['magType'],
                    'nst': row['nst'],
                    'gap': row['gap'],
                    'dmin': row['dmin'],
                    'rms': row['rms'],
                    'net': row['net'],
                    'id_earthquake': row['id'],
                    'updated': row['updated'],
                    'place': row['place'],
                    'type': row['type'],
                    'local_time': row['local_time']
                })
        return redirect(url_for('index'))
    return 'No file uploaded', 400

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        min_mag = request.form.get('min_mag')
        max_mag = request.form.get('max_mag')
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        lat = request.form.get('latitude')
        lon = request.form.get('longitude')
        place = request.form.get('place')
        night_time = request.form.get('night_time')

        query = 'SELECT * FROM earthquakes WHERE 1=1'
        params = {}

        if min_mag and max_mag:
            query += ' AND Magnitude BETWEEN :min_mag AND :max_mag'
            params['min_mag'] = min_mag
            params['max_mag'] = max_mag

        if start_date and end_date:
            if start_date <= end_date:
                query += ' AND DateTime BETWEEN :start_date AND :end_date'
                params['start_date'] = start_date
                params['end_date'] = end_date
            else:
                return 'Error: Start date must be before end date.', 400

        if lat and lon:
            query += ' AND Latitude = :latitude AND Longitude = :longitude'
            params['latitude'] = lat
            params['longitude'] = lon

        if place:
            query += ' AND Place LIKE :place'
            params['place'] = f'%{place}%'

        if night_time:
            query += " AND Magnitude > 4.0 AND (CAST(time AS TIME) >= '18:00:00' OR CAST(time AS TIME) <= '06:00:00')"

        # print(query)  # Debugging: print the query
        # print(params)  # Debugging: print the parameters

        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            earthquakes = result.fetchall()
            # print(earthquakes)  # Debugging: Print the result set
            return render_template('results.html', earthquakes=earthquakes)

    return render_template('query.html')


@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    with engine.connect() as connection:
        result = connection.execute(text('SELECT COUNT(*) AS count FROM earthquakes WHERE Magnitude > 5.0'))
        count = result.fetchone()[0]  # Accessing the first column directly
    return f'Total earthquakes with magnitude greater than 5.0: {count}'

@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    with engine.connect() as connection:
        result = connection.execute(text('''
            SELECT COUNT(*) AS count FROM earthquakes 
            WHERE Magnitude > 4.0 
            AND (CAST(time AS TIME) >= '18:00:00' OR CAST(time AS TIME) <= '06:00:00')
        '''))
        count = result.fetchone()[0]  # Accessing the first column directly
    return f'Total large earthquakes (>4.0 mag) at night: {count}'

if __name__ == '__main__':
    app.run(debug=True) 
