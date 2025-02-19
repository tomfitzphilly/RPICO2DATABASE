import sqlite3
import socket
import json
from datetime import datetime
# import threading
sensor_data = {}
# Database setup
def init_database():
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co2 REAL NOT NULL,
            device_id TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            temperature REAL NOT NULL,
            humidity REAL NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def store_reading(sensor_data): 
    # key_list = ('co2', 'device_id', 'timestamp', 'temperature', 'humidity')
    print(" store_dict is: ", sensor_data) 
    
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO sensor_readings (co2, device_id, timestamp, 
                temperature, humidity) VALUES (?, ?, ?, ?, ?)''', (sensor_data['co2'], 
                sensor_data['device_id'], sensor_data['timestamp'], sensor_data['temperature'], 
                sensor_data['humidity']))

    conn.commit()
    cursor.close()
    conn.close()

def handle_client(client_socket, addr):
    try:
        while True:
            data = client_socket.recv(1024)
            print("incoming data is: ", str(data))
            print("Data type: ", type(data))
            
            if not data:
                break
            
            try:
                sensor_data = json.loads(data)
                print("handle client reading is : ", str(sensor_data))
                store_reading(sensor_data)
                client_socket.send(b'OK')
            except json.JSONDecodeError:
                client_socket.send(b'ERROR: Invalid JSON')
            except KeyError:
                client_socket.send(b'ERROR: Missing required fields')
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client_socket.close()

def main():
    init_database()
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', 8080))
    server_socket.listen(5)
    
    print("Server listening on port 8080...")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"New connection from {addr}")
        handle_client(client_socket, addr)
        store_reading(sensor_data)

if __name__ == '__main__':
    main()
