import os
import pandas as pd
import pymysql
from flask import Flask, request, render_template, redirect, url_for

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generates a random secret key

# Connection details
host = 'manoharb.database.windows.net'
port = 1433
user = 'manoharb'
password = 'Arjunsuha1*'
db = 'manoharbontha'

# Database connection
conn = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    db=db,
    cursorclass=pymysql.cursors.DictCursor,
    connect_timeout=30,  # Increase the timeout value
    read_timeout=30,  # Increase the read timeout value
    write_timeout=30  # Increase the write timeout value
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        data = pd.read_csv(file)
        cursor = conn.cursor()
        for index, row in data.iterrows():
            cursor.execute('''
                INSERT INTO Earthquake (
                    time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (row['time'], row['latitude'], row['longitude'], row['depth'], row['mag'], row['magType'],
            row['nst'], row['gap'], row['dmin'], row['rms'], row['net'], row['id'],
            row['updated'], row['place'], row['type']))
        conn.commit()
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

        cursor = conn.cursor()

        if mag:
            cursor.execute('SELECT * FROM Earthquake WHERE mag >= %s', (mag,))
            earthquakes = cursor.fetchall()
            return render_template('results.html', earthquakes=earthquakes)

        if date_range:
            start_date, end_date = date_range.split(' - ')
            cursor.execute('SELECT * FROM Earthquake WHERE time BETWEEN %s AND %s', (start_date, end_date))
            earthquakes = cursor.fetchall()
            return render_template('results.html', earthquakes=earthquakes)

        if lat and lon and distance:
            lat, lon, distance = float(lat), float(lon), float(distance)
            cursor.execute('''
                SELECT *, 
                (111.045 * DEGREES(ACOS(COS(RADIANS(%s)) 
                * COS(RADIANS(latitude)) 
                * COS(RADIANS(%s) - RADIANS(longitude)) 
                + SIN(RADIANS(%s)) 
                * SIN(RADIANS(latitude))))) AS distance
                FROM Earthquake
                HAVING distance <= %s
            ''', (lat, lon, lat, distance))
            earthquakes = cursor.fetchall()
            return render_template('results.html', earthquakes=earthquakes)

    return render_template('query.html')

@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) AS count FROM Earthquake WHERE mag > 5.0')
    count = cursor.fetchone()['count']
    return f'Total earthquakes with magnitude greater than 5.0: {count}'

@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) AS count FROM Earthquake 
        WHERE mag > 4.0 
        AND (TIME(time) >= '18:00:00' OR TIME(time) <= '06:00:00')
    ''')
    count = cursor.fetchone()['count']
    return f'Total large earthquakes (>4.0 mag) at night: {count}'

if __name__ == '__main__':
    app.run(debug=True)
