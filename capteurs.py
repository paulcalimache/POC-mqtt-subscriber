from pymongo import MongoClient
import time
import random
from paho.mqtt import client as mqtt_client

broker = 'broker.emqx.io' # Broker gratuit ouvert à tout le monde
port = 1883 # Port par défaut
topic = "cesi"
client_id = "paulcalimache"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)
        
def connect_mqtt() -> mqtt_client:
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def sendMeasures(client: mqtt_client):
    mesure = "Temperature : " + str(random.randrange(-10, 40, 1) + "°")
    client.publish(topic, mesure)
    print("> SENT : " + mesure)

client = connect_mqtt()
client.loop_start()
while True:
    sendMeasures(client)
    time.sleep(5)
    