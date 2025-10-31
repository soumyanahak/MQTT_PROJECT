import random
import time
import ujson
from umqtt.simple import MQTTClient
import network

# --- MQTT Configuration ---
# The MQTT broker's address. 'test.mosquitto.org' is a public, free broker.
MQTT_BROKER = "test.mosquitto.org"
# The base topic for all communication in this project.
BASE_TOPIC = "sensors"

# --- Client Management ---
# A dictionary to keep track of active clients.
# Each key is a client ID (string), and the value is another dictionary
# containing the sensors the client is subscribed to and the last time they were active.
active_clients = {}  # {client_id: {sensors: [list], last_seen: timestamp}}
# A list of all available "sensor types" for the clients to subscribe to.
# These will be the topics the data is published on.
sensor_types = ["temperature", "humidity", "light", "distance", "potentiometer", "button"]

# --- Function Definitions ---

def connect_wifi():
    """
    Connects the device to the specified Wi-Fi network.
    This is a rewritten function to use the specified SSID and password.
    """
    wifi = network.WLAN(network.STA_IF)
    wifi.active(True)
    # Connect to the Wi-Fi network with the provided credentials.
    wifi.connect('soumyawifi', '12345678')
    print("Connecting to WiFi...")
    # Wait until the device is connected to the Wi-Fi.
    while not wifi.isconnected():
        time.sleep(0.5)
    print("WiFi Connected!")

def message_callback(topic, msg):
    """
    This function is called automatically when a message is received on a subscribed topic.
    It handles incoming MQTT messages, specifically from the control topic.
    """
    try:
        # Decode the topic and message from bytes to a string.
        topic_str = topic.decode()
        message = ujson.loads(msg.decode())

        # Check if the message is on the control topic.
        if topic_str == f"{BASE_TOPIC}/control":
            # If it is, pass the message to the handler function.
            handle_control_message(message)

    except Exception as e:
        # Print any errors that occur during message processing.
        print(f"Message callback error: {e}")

def handle_control_message(message):
    """
    Processes control commands from clients, such as subscribe, unsubscribe, and disconnect.
    This is the core logic for the "broker-centric" model.
    """
    # Get the action and client ID from the incoming message.
    action = message.get("action")
    client_id = message.get("client")

    # If no client ID is provided, it's an invalid command.
    if not client_id:
        print("No client ID provided")
        return

    # A series of conditional checks to determine the action.
    if action == "subscribe":
        # Get the list of sensors the client wants to subscribe to.
        sensors = message.get("sensors", [])
        subscribe_client(client_id, sensors)

    elif action == "unsubscribe":
        # Get the list of sensors the client wants to unsubscribe from.
        sensors = message.get("sensors", [])
        unsubscribe_client(client_id, sensors)

    elif action == "disconnect":
        # Disconnect the client and remove them from the active list.
        disconnect_client(client_id)

    elif action == "ping":
        # Update the client's last_seen timestamp to keep them active.
        ping_client(client_id)

def subscribe_client(client_id, sensors):
    """
    Subscribes a client to a list of specified sensors.
    This function manages the 'active_clients' dictionary.
    """
    # Filter the requested sensors to ensure they are valid sensor types.
    valid_sensors = [s for s in sensors if s in sensor_types]

    # If the client is not already in the active list, add them.
    if client_id not in active_clients:
        active_clients[client_id] = {"sensors": [], "last_seen": time.time()}

    # Add any new valid sensors to the client's subscription list.
    current_sensors = active_clients[client_id]["sensors"]
    for sensor in valid_sensors:
        if sensor not in current_sensors:
            current_sensors.append(sensor)

    # Update the 'last_seen' timestamp for the client.
    active_clients[client_id]["last_seen"] = time.time()

    print(f"Client {client_id} subscribed to: {valid_sensors}")

    # Create a confirmation message to publish back to the client.
    response = {
        "client": client_id,
        "action": "subscribed",
        "sensors": active_clients[client_id]["sensors"],
        "timestamp": time.time()
    }
    # Publish the confirmation message to a client-specific topic with QoS 1.
    mqtt.publish(f"{BASE_TOPIC}/clients/{client_id}", ujson.dumps(response), qos=1)

def unsubscribe_client(client_id, sensors):
    """
    Unsubscribes a client from a list of specified sensors.
    """
    # If the client is not active, there's nothing to do.
    if client_id not in active_clients:
        return

    # Remove the specified sensors from the client's subscription list.
    current_sensors = active_clients[client_id]["sensors"]
    for sensor in sensors:
        if sensor in current_sensors:
            current_sensors.remove(sensor)

    # Update the 'last_seen' timestamp.
    active_clients[client_id]["last_seen"] = time.time()

    print(f"Client {client_id} unsubscribed from: {sensors}")

    # Create and publish a confirmation message with QoS 1.
    response = {
        "client": client_id,
        "action": "unsubscribed",
        "sensors": active_clients[client_id]["sensors"],
        "timestamp": time.time()
    }
    mqtt.publish(f"{BASE_TOPIC}/clients/{client_id}", ujson.dumps(response), qos=1)

def disconnect_client(client_id):
    """
    Disconnects a client completely by removing their entry from the active clients dictionary.
    """
    if client_id in active_clients:
        # Remove the client from the dictionary.
        del active_clients[client_id]
        print(f"Client {client_id} disconnected")

        # Create and publish a disconnection notice with QoS 1.
        response = {
            "client": client_id,
            "action": "disconnected",
            "timestamp": time.time()
        }
        mqtt.publish(f"{BASE_TOPIC}/clients/{client_id}", ujson.dumps(response), qos=1)

def ping_client(client_id):
    """
    Responds to a client's ping request to keep their 'last_seen' timestamp fresh.
    """
    if client_id in active_clients:
        # Update the 'last_seen' timestamp.
        active_clients[client_id]["last_seen"] = time.time()

    # Create and publish a ping response message with QoS 1.
    response = {
        "client": client_id,
        "action": "ping",
        "timestamp": time.time()
    }
    mqtt.publish(f"{BASE_TOPIC}/clients/{client_id}", ujson.dumps(response), qos=1)

def connect_mqtt():
    """
    Connects to the MQTT broker, sets the message callback, and subscribes to the control topic.
    """
    # Declare `mqtt` as a global variable so it can be accessed throughout the program.
    global mqtt
    # Create an MQTT client instance. The client ID is "mqtt-simulator".
    mqtt = MQTTClient("mqtt-simulator", MQTT_BROKER)
    # Set the function to be called when a message is received.
    mqtt.set_callback(message_callback)
    # Connect to the MQTT broker.
    mqtt.connect()

    # Subscribe to the main control topic with QoS 1 where clients send their requests.
    mqtt.subscribe(f"{BASE_TOPIC}/control", qos=1)
    print("MQTT Connected and subscribed to control topic with QoS 1!")
    return mqtt

def read_simulated_sensors():
    """
    Generates random data for each "sensor" to simulate readings from a device.
    This replaces the original function that read from physical hardware.
    """
    return {
        # Generate random integer for temperature between 20 and 30.
        "temperature": random.randint(20, 30),
        # Generate random integer for humidity between 40 and 60.
        "humidity": random.randint(40, 60),
        # Generate random integer for light between 0 and 1000.
        "light": random.randint(0, 1000),
        # Generate random integer for distance between 5 and 500.
        "distance": random.randint(5, 500),
        # Generate random integer for potentiometer between 0 and 4095.
        "potentiometer": random.randint(0, 4095),
        # Generate a random boolean value for the button state.
        "button": random.choice([True, False])
    }

def publish_sensor_data(sensor_data):
    """
    Publishes the simulated sensor data to the appropriate topics, but only
    for sensors that have at least one subscriber.
    """
    timestamp = time.time()

    # Iterate through each sensor type and its value.
    for sensor_type, value in sensor_data.items():
        subscribers = []
        # Find which clients are subscribed to the current sensor type.
        for client_id, client_info in active_clients.items():
            if sensor_type in client_info["sensors"]:
                subscribers.append(client_id)

        # Only publish if there are active subscribers for this sensor.
        if subscribers:
            # Create a JSON payload with the sensor data, timestamp, and subscriber list.
            data = {
                "sensor": sensor_type,
                "value": value,
                "timestamp": timestamp,
                "subscribers": subscribers
            }

            # Define the specific data topic for this sensor.
            topic = f"{BASE_TOPIC}/data/{sensor_type}"
            # Publish the JSON data to the topic with QoS 1.
            mqtt.publish(topic, ujson.dumps(data), qos=1)

    # Publish a summary of the current state for general monitoring purposes with QoS 1.
    summary = sensor_data.copy()
    summary["timestamp"] = timestamp
    summary["active_clients"] = len(active_clients)
    mqtt.publish(f"{BASE_TOPIC}/summary", ujson.dumps(summary), qos=1)

def cleanup_inactive_clients():
    """
    Removes clients that have not sent a ping or subscription message within a timeout period.
    This helps keep the `active_clients` list clean.
    """
    current_time = time.time()
    inactive_clients = []

    # Check each client to see if their last_seen time is older than the timeout (5 minutes).
    for client_id, client_info in active_clients.items():
        if current_time - client_info["last_seen"] > 300:  # 5 minutes timeout
            inactive_clients.append(client_id)

    # Disconnect any inactive clients found.
    for client_id in inactive_clients:
        disconnect_client(client_id)
        print(f"Removed inactive client: {client_id}")


def main():
    """
    The main function that initializes the system and runs the primary loop.
    """
    global mqtt

    # Start the process by connecting to Wi-Fi and then the MQTT broker.
    connect_wifi()
    mqtt = connect_mqtt()

    print("MQTT Simulator started!")
    print(f"Control topic: {BASE_TOPIC}/control")
    print(f"Data topics: {BASE_TOPIC}/data/{{sensor_type}}")
    print("\nAvailable sensors:", sensor_types)

    last_cleanup = time.time()

    # The main loop that runs indefinitely.
    while True:
        try:
            # Check for incoming MQTT messages.
            mqtt.check_msg()
            # Read the simulated sensor data.
            sensor_data = read_simulated_sensors()
            # Publish the data for subscribed clients.
            publish_sensor_data(sensor_data)

            # Print a status update to the console.
            button_status = "PRESSED" if sensor_data['button'] else "Released"
            print(f"Temp: {sensor_data['temperature']}°C, Humidity: {sensor_data['humidity']}%, Light: {sensor_data['light']}, Distance: {sensor_data['distance']}cm, Button: {button_status}, Clients: {len(active_clients)}")

            # Check if it's time to clean up inactive clients.
            if time.time() - last_cleanup > 60:
                cleanup_inactive_clients()
                last_cleanup = time.time()

            # Pause for 2 seconds before the next loop iteration.
            time.sleep(2)

        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(1)

# The standard entry point for a Python script.
if __name__ == "__main__":
    main()