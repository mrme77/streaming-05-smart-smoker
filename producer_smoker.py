import csv
import pika
import time
import webbrowser

# RabbitMQ configuration
rabbit_host = 'localhost'
queues = {
    '01-smoker': 'Channel1',
    '02-food-A': 'Channel2',
    '03-food-B': 'Channel3'
}

def open_rabbitmq_admin_site(host='localhost', port=15672):
    """
    Open the RabbitMQ Admin website in a web browser.

    Args:
        host (str): The RabbitMQ server host (default is 'localhost').
        port (int): The port where the RabbitMQ Admin website is hosted (default is 15672).
    """
    admin_url = f"http://{host}:{port}/#/"
    webbrowser.open_new(admin_url)

def send_temperature_to_queue(temperature, queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    # Create a message with temperature data
    message = str(temperature)

    # Send the message to the specified queue
    channel.basic_publish(exchange='', routing_key=queue_name, body=message, properties=pika.BasicProperties(
        delivery_mode=2,  # Make the message persistent
    ))

    print(f" [x] Sent temperature {temperature} to '{queue_name}'")
    connection.close()



if __name__ == '__main__':
    # Open RabbitMQ Admin site
    open_rabbitmq_admin_site()

    # Read temperature data from CSV file and send to RabbitMQ
    with open('smoker-temps.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            for queue_name, channel_name in queues.items():
                temperature = float(row[channel_name])
                send_temperature_to_queue(temperature, queue_name)
            
            time.sleep(30)  # Sleep for 30 seconds (half a minute)
