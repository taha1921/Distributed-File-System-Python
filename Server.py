import socket
import thread
import os

def func(client, addr):
    print "connected to: ", addr

def main():
    ip = '127.0.0.1'
    port = 50184
    s = socket.socket()
    s.bind((ip, port))
    s.listen(10) # max number of connections
    print "waiting for connections"
    while True:
		client, addr = s.accept()
		thread.start_new_thread(func,(client,addr))

if __name__ == '__main__':
    main()