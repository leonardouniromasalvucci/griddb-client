import paho.mqtt.client as mqtt
import time, sys, os
from threading import Thread
import time, random, json
import keyboard

n_devices = 1

class Message:
  def __init__(self, timestamp, device_id, value):
	  self.timestamp = timestamp
	  self.device_id = device_id
	  self.value = value

def on_connect(client, userdata, flags, rc):
	if rc==0:
		print("Connected with returned code = ", rc)
	else:
		print("Bad connection with returned code = ", rc)
	return rc
	
class Device(Thread):

	def __init__(self, id):
		Thread.__init__(self)
		self.id = id
	
	def run(self):
		print ("Device " + str(self.id) + " avviato")
		mqtt.Client.connected_flag = False
		broker = "KalpaELB-10280071324ea8de.elb.eu-west-1.amazonaws.com"
		client = mqtt.Client()
		client.on_connect = on_connect
		while True:
			try:
				print("Connecting to broker ", broker)
				client.connect(broker)
				m = json.dumps(Message(time.time(), str(self.id), str(round(random.uniform(0.5, 1.9),3))).__dict__)
				client.publish("0001/", m, 2)
				print("Device "+ str(self.id) + "has published: " + m)
				time.sleep(5)
				client.disconnect()
			except:
				print("Trying connection...")
				time.sleep(5)
				pass

for devices_id in range(1, n_devices+1):
	try:
		new_device = Device(devices_id)
		new_device.daemon = True
		new_device.start()

		time.sleep(random.uniform(0, 5))
	except:
		print("ERROR")
		sys.exit(1)

while True:
	try:
		pass
	except KeyboardInterrupt:
		sys.exit()