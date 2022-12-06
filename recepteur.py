from pymongo import MongoClient
from paho.mqtt import client as mqtt_client

broker = 'broker.emqx.io' # Broker gratuit ouvert à tout le monde
port = 1883 # Port par défaut
topic = "cesi"
client_id = "paulcalimache"
mongoCon = MongoClient("mongodb://localhost:27017")
db = mongoCon["mqttDatabase"]
coll = db["messages"]

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    msgReceived = msg.payload.decode()
    print("> ", msgReceived)
    msgJSON = {
        "topic": topic,
        "message": msgReceived,
    }
    coll.insert_one(msgJSON)
        
def connect_mqtt() -> mqtt_client:
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    client.subscribe(topic)
    client.on_message = on_message

client = connect_mqtt()
subscribe(client)
client.loop_forever()



