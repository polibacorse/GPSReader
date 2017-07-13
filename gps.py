import bitstring
import pynmea2
import serial
import time

import paho.mqtt.client as mqtt

MQTT_TOPIC = '$SYS/raw'
client = None
speed = str(bytes([0])) # buffer

def millis():
    return int(round(time.time() * 1000))

def init_mqtt():
    global client
    client = mqtt.Client()
    client.connect_async('localhost')
    client.loop_start()

def checkSign(check, val, bitarr):
    if check == val:
        bitarr.set(0, 0)
    else:
        bitarr.set(1, 0)

def main():
    global speed
    init_mqtt()
    device = serial.Serial('/dev/ttyUSB0', 38400)

    while True:
        data_raw = device.readline().rstrip()
        msg = data_raw.decode('ascii', 'ignore')
        try:
            msg = pynmea2.parse(msg)
        except pynmea2.ParseError:
            continue # readline missed something

        if isinstance(msg, pynmea2.GGA):
            b = bitstring.pack('>d', msg.latitude)
            checkSign(msg.lat_dir, 'N', b)
            lat_arr = []
            i = 0;
            while i < len(b):
                lat_arr.append(b[i:i+8].int)
                i = i + 8
            #print('GGA-LAT: ', msg.latitude, lat_arr)


            b = bitstring.pack('>d', msg.longitude)
            checkSign(msg.lon_dir, 'E', b)
            lon_arr = []
            i = 0;
            while i < len(b):
                lon_arr.append(b[i:i+8].int)
                i = i + 8
            #print('GGA-LON: ', msg.longitude, lon_arr)

            altitude = str(bytes([int(msg.altitude)]))
            sats = str(bytes([int(msg.num_sats)]))
            fix = str(bytes([int(msg.gps_qual)]))
            #print('GGA-ASF: A', msg.altitude, 'S', msg.num_sats, 'F', msg.gps_qual)
            client.publish(
                MQTT_TOPIC, '{{"id":752,"time":{},"data":{}}}'.format(
                    millis(), ','.join([altitude, speed, sats, fix])))


        if isinstance(msg, pynmea2.RMC):
            b = bitstring.pack('>d', msg.latitude)
            checkSign(msg.lat_dir, 'N', b)
            lat_arr = []
            i = 0;
            while i < len(b):
                lat_arr.append(b[i:i+8].int)
                i = i + 8
            #print('RMC-LAT: ', msg.latitude, lat_arr)
            client.publish(
                MQTT_TOPIC, '{{"id":753,"time":{},"data":{}}}'.format(
                    millis(), lat_arr))

            b = bitstring.pack('>d', msg.longitude)
            checkSign(msg.lon_dir, 'E', b)
            lon_arr = []
            i = 0;
            while i < len(b):
                lon_arr.append(b[i:i+8].int)
                i = i + 8
            #print('RMC-LON: ', msg.longitude, lon_arr)
            client.publish(
                MQTT_TOPIC, '{{"id":754,"time":{},"data":{}\}}'.format(
                    millis(), lon_arr))

            speed = str(bytes([int(msg.spd_over_grnd * 1.852)]))
            #print('RMC-speed: ', speed)
            # missing time - see msg.timestamp

if __name__ == '__main__':
    main()
