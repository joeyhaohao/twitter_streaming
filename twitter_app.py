import socket
import json
import _thread
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
HOST = "localhost"
PORT = 9001

class TweetsStreamListener(StreamListener):

    def __init__(self, client, addr):
        self.client = client
        self.addr = addr

    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print("------------------------------------------")
            print(msg['text'])
        except BaseException as e:
            print("Error receiving data: %s" % str(e))
            return True

        # send to spark client
        try:
            # tweet = msg['text'] + '|'
            # self.client.send(tweet.encode('utf-8'))
            self.client.send("china", msg['text'].encode('utf-8'))
        except BaseException as e:
            print("Error sending data to client %s: %s" % (self.addr, str(e)))
            return False
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False
        # returning non-False reconnects the stream, with backoff.

def create_listener(client, addr):
    print("Client connected", addr)
    streamListener = TweetsStreamListener(client, addr)
    stream = Stream(auth=auth, listener=streamListener)
    stream.filter(track=['china'])  #languages filter cause IncompleteRead error

if __name__ == "__main__":
    # Create a socket object
    sock = socket.socket()
    # Bind to the port
    sock.bind((HOST, PORT))
    sock.listen(5)
    print("Listening on port: %s" % PORT)

    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    create_listener(producer, 9092)

    # send to multiple clients using sockets
    # while True:
    #     client, addr = sock.accept()
    #     _thread.start_new_thread(create_listener, (client, addr))
