import socket
import tweepy
import json
import _thread

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
HOST = "localhost"
PORT = 9001

class TweetsStreamListener(tweepy.StreamListener):

    def __init__(self, client, addr):
        self.client = client
        self.addr = addr

    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        msg = None
        try:
            msg = json.loads(data)
            print("------------------------------------------")
            print(msg['text'])
        except BaseException as e:
            print("Error receiving data: %s" % str(e))

        # client closed
        try:
            tweet = msg['text'] + '|'
            self.client.send(tweet.encode('utf-8'))
        except BaseException as e:
            print("Error sending data to client %s: %s" % (addr, str(e)))
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
    stream = tweepy.Stream(auth=auth, listener=streamListener)
    stream.filter(languages=['en'], track=['life'])

if __name__ == "__main__":
    # Create a socket object
    sock = socket.socket()
    # Bind to the port
    sock.bind((HOST, PORT))
    sock.listen(5)
    print("Listening on port: %s" % PORT)

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    while True:
        client, addr = sock.accept()
        _thread.start_new_thread(create_listener, (client, addr))
