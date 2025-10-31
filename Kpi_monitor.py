# Import necessary libraries for a standard Python environment.
import paho.mqtt.client as mqtt
import json
import time

# --- MQTT Configuration ---
# The same broker address as your Wokwi simulation.
MQTT_BROKER = "test.mosquitto.org"
# The base topic for all communication.
BASE_TOPIC = "sensors"

# --- KPI Measurement Variables ---
# A list to store latency values for each received message.
latencies = []
# A counter for the total number of data messages received.
message_count = 0
# The time when the script started, used for throughput calculation.
start_time = time.time()
# The last known number of active clients, based on the summary topic.
active_clients = 0

def on_connect(client, userdata, flags, rc):
    """
    This function is called when the client successfully connects to the MQTT broker.
    It subscribes to the relevant topics for KPI measurement.
    """
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to all data topics using the '#' wildcard. This ensures we receive all sensor data.
        client.subscribe(f"{BASE_TOPIC}/data/#")
        # Subscribe to the summary topic to track the number of active clients.
        client.subscribe(f"{BASE_TOPIC}/summary")
        print("Subscribed to all data and summary topics.")
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    """
    This function is called automatically whenever a message is received.
    It processes the message and calculates the KPIs.
    """
    global message_count
    global active_clients

    try:
        # Decode the received JSON message.
        payload = json.loads(msg.payload.decode())
        topic = msg.topic
        
        # --- Check the message topic to determine which KPI to measure ---
        
        # If the message is from a sensor data topic (e.g., sensors/data/temperature).
        if topic.startswith(f"{BASE_TOPIC}/data/"):
            # Increment the total number of messages received.
            message_count += 1
            
            # Get the timestamp from the message payload.
            message_timestamp = payload.get("timestamp")
            
            # Calculate latency if a timestamp is present.
            if message_timestamp:
                latency = time.time() - message_timestamp
                # Add the new latency measurement to our list.
                latencies.append(latency)
                
                # --- Print real-time KPI data ---
                print(f"--- Data Received on Topic: {topic} ---")
                print(f"Sensor Value: {payload.get('value')}")
                print(f"Subscribed Clients: {payload.get('subscribers', 'N/A')}")
                print(f"Latency: {latency:.4f} seconds")
                
                # Calculate and print the average latency.
                if latencies:
                    avg_latency = sum(latencies) / len(latencies)
                    print(f"Average Latency: {avg_latency:.4f} seconds")

                # Calculate and print throughput.
                elapsed_time = time.time() - start_time
                if elapsed_time > 0:
                    throughput = message_count / elapsed_time
                    print(f"Throughput: {throughput:.2f} messages/sec")
                print("-" * 30)

        # If the message is from the summary topic.
        elif topic == f"{BASE_TOPIC}/summary":
            # Update the count of active clients.
            active_clients = payload.get("active_clients", 0)
            print(f"--- System Summary Update ---")
            print(f"Total Active Clients: {active_clients}")
            print("-" * 30)
            
    except json.JSONDecodeError:
        print("Failed to decode JSON from message.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Create an MQTT client instance.
client = mqtt.Client()
# Set the `on_connect` function to be called on connection.
client.on_connect = on_connect
# Set the `on_message` function to be called on message reception.
client.on_message = on_message

# Connect to the MQTT broker.
client.connect(MQTT_BROKER, 1883, 60)

# Start a loop to listen for incoming messages and process them.
client.loop_forever()
