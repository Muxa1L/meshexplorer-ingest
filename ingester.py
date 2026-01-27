import paho.mqtt.client as mqtt
from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.crypto import MeshCoreKeyStore
from meshcoredecoder.types.crypto import DecryptionOptions
from meshcoredecoder.types.enums import PayloadType
from meshcoredecoder.utils.enum_names import get_route_type_name, get_payload_type_name, get_device_role_name
import json
import clickhouse_connect
from datetime import datetime
import binascii
import os

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

clickhouse_host = str(os.environ['CLICKHOUSE_HOST'])
clickhouse_port = int(os.environ['CLICKHOUSE_PORT'])
clickhouse_user = str(os.environ['CLICKHOUSE_USER'])
clickhouse_pass = str(os.environ['CLICKHOUSE_PASS'])

mqtt_host = str(os.environ['MQTT_HOST'])
mqtt_port = int(os.environ['MQTT_PORT'])
mqtt_user = str(os.environ['MQTT_USER'])
mqtt_pass = str(os.environ['MQTT_PASS'])
mqtt_topic = str(os.environ['MQTT_TOPIC'])

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(f"{mqtt_topic}/+/raw")
# def get_connection():
    
def get_client():
    return clickhouse_connect.get_client(host=clickhouse_host, port=clickhouse_port, username=clickhouse_user, password=clickhouse_pass)

def insert_advert(packet_data):
    advert = MeshCoreDecoder.decode(packet_data['data']).to_dict()
    if (not advert['isValid']):
        print(f"invalid packet\n{str(advert)}")
        return
    client = get_client()
    columns = ['mesh_timestamp', 'adv_timestamp', 'public_key', 'node_name', 'has_location']
    data = []
    data.append(advert["payload"]["decoded"]["timestamp"]*1000)
    data.append(advert["payload"]["decoded"]["timestamp"]*1000)
    data.append(advert["payload"]["decoded"]["publicKey"])
    data.append(advert["payload"]["decoded"]["appData"]["name"])
    data.append(advert["payload"]["decoded"]["appData"]["hasLocation"])
    if (advert["payload"]["decoded"]["appData"]["hasLocation"]):
        columns.extend(['latitude', 'longitude', 'altitude'])
        data.append(advert["payload"]["decoded"]["appData"]["location"]["latitude"])
        data.append(advert["payload"]["decoded"]["appData"]["location"]["longitude"])
        data.append(0) #altitude
    
    match advert["payload"]["decoded"]["appData"]["deviceRole"]:
        case 1:
            columns.append('is_chat_node')
            data.append(True)
        case 2:
            columns.append('is_repeater')
            data.append(True)
        case 3:
            columns.append('is_room_server')
            data.append(True)
    columns.extend(['has_name', 'broker', 'topic'])
    data.append(advert["payload"]["decoded"]["appData"]["hasName"])
    data.append(f"tcp://{mqtt_host}:{mqtt_port}")
    data.append(mqtt_topic.lower())
    if (advert["pathLength"] > 0):
        columns.append('path')
        data.append(binascii.unhexlify(''.join(advert["path"])))
    data.append(advert["pathLength"])
    data.append(binascii.unhexlify(packet_data['origin_id']))
    data.append('Mux')
    data.append(binascii.unhexlify(advert["messageHash"]))
    columns.extend(['path_len', 'origin_pubkey', 'origin', 'packet_hash'])
    client.insert('meshcore_adverts', [data], column_names=columns)

def insert_packet(packet_data):
    packet = MeshCoreDecoder.decode(packet_data['data']).to_dict()
    if (not packet['isValid']):
        print(f"invalid packet\n{str(packet_data)}")
        return
    client = get_client()
    columns = ['mesh_timestamp', 'broker', 'topic', 'origin', 'origin_pubkey', 'message_hash', 'packet', 'payload', 'route_type', 'payload_type', 'payload_version', 'header', 'path_len']
    data = []
    data.append(packet_data['timestamp'])
    data.append(f"tcp://{mqtt_host}:{mqtt_port}")
    data.append(mqtt_topic.lower())
    data.append(packet_data['origin'])
    data.append(binascii.unhexlify(packet_data['origin_id']))
    data.append(packet['messageHash'])
    data.append(binascii.unhexlify(packet_data['data']))
    
    data.append(binascii.unhexlify(packet['payload']['raw']))
    data.append(packet['routeType'])               
    data.append(packet['payloadType'])               
    data.append(packet['payloadVersion'])               
    data.append(packet_data['data'][:2]) # header
    
    data.append(packet["pathLength"])
    if (packet["pathLength"] > 0):
        columns.append('path')
        data.append(binascii.unhexlify(''.join(packet["path"])))
    # data.append()               
    # data.append()      
    client.insert('meshcore_packets', [data], column_names=columns)
    
# unused for now
# def insert_public_channel_message(packet, mqtt_data):
#     client = get_client()
#     columns = ['mesh_timestamp', 'channel_hash', 'mac', 'encrypted_message', 'message_id', 'origin_path_info']
#     data = []
#     data.append(mqtt_data['timestamp'])
#     data.append(packet['payload']['raw'][:2])
#     data.append(packet['payload']['raw'][2:6])
#     data.append(packet['payload']['raw'][6:])
#     # data.append(1)
#     data.append(packet['messageHash'])
#     origin_path_info = []
#     origin_path_info.append(mqtt_data['origin'])
#     origin_path_info.append(binascii.unhexlify(mqtt_data['origin_id']))
#     if (packet["pathLength"] > 0):
#         origin_path_info.append(binascii.unhexlify(''.join(packet["path"])))
#     else:
#         origin_path_info.append('')
#     origin_path_info.append("tcp://meshcore:meshcore@192.168.1.20:1883")
#     origin_path_info.append("meshcore/krr")
#     data.append([tuple(e for e in origin_path_info)])
#     print(data)
#     client.insert('meshcore_public_channel_messages', [data], column_names=columns)
   
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    hex_data = data['data']
    try:
        packet = MeshCoreDecoder.decode(hex_data)
        insert_packet(data)
        # 
        if packet.payload_type == PayloadType.Advert and packet.payload.get('decoded'):
            insert_advert(packet.to_dict())
    except Exception as exc:
        print(data)
        print(exc)
    

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username = mqtt_user
mqttc.password = mqtt_pass
mqttc.connect(mqtt_host, mqtt_port, 60)
mqttc.loop_forever()
