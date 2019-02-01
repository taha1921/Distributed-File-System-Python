import socket
import thread
import os
import random

clients = []
chunk_servers = {}
files = {}
chunk_servers_addr = []

def identify(client, addr):
    client.send("identify")
    string = client.recv(1024)
    if(string == "client"):
        clients.append(client)
        print 'connected to client', addr
        menu(client)
    elif string == "chunk_server":
        client.send(str(addr[1]))
        chunk_servers_addr.append(str(addr[1]))
        chunk_servers[str(addr[1])] = client
        print 'connected to chunk server', addr



def menu(client):
    client.send("would you like to read or write a file")
    response = client.recv(1024)
    while (response != 'read' and response != 'write'):
        client.send('invalid response, choose between reads and write')
        response = client.recv(1024)
    
    if(response == 'read'):
        readfunc(client)
    if(response == 'write'):
        writefunc(client)

def readfunc(client):
    client.send('what file do you want to read')
    filename = client.recv(1024)
    print files
    if(files.has_key(filename)):
        client.send('starting read')
        file = files[filename]
        client.recv(1024)
        i = 0
        for chunk in file:
            sock = chunk[1]
            client.send(sock)
            client.recv(1024)
            c = chunk[0]
            client.send(str(c))
            client.recv(1024)
            if(i == (len(file) - 1)):
                client.send('FINISHED')
            else:
                client.send('sending next chunk')
                client.recv(100)
            i = i + 1
        client.recv(10)
    else:
        client.send('file does not exist')
        client.recv(100)
    
    menu(client)


def writefunc(client):
    client.send("To write to an existing file, press 1, To create a new file, press 2")
    resp = client.recv(1024)
    resp = int(resp)
    if (resp == 1):
        existingfile(client)
    else:
        createfile(client)

def existingfile(client):
    client.send("Enter name of file you want to write to")
    filename = client.recv(1024)
    if filename not in files:
        client.send('file does not exist')
        client.recv(80)
        menu(client)
    else:
        client.send('do you want to write in the middle of the file (PRESS 1) or at the end (PRESS 2)')
        choice = client.recv(80)
        choice = int(choice)
        if choice == 1:
            middle(client, filename)
        else:
            end(client, filename)

def middle(client, filename):
    client.send('What byte offset do you want to write to')
    offset = client.recv(1024)
    offset = int(offset)
    file = files[filename]
    totalsize = 0
    for x in file:
        totalsize = totalsize + x[2]

    if offset > totalsize:
        end(client, filename)
    else:
        for x in file:
            if(offset >= x[2]):
                offset = offset - x[2]
            else:
                client.send(x[1])
                client.recv(100)
                client.send(x[3])
                client.recv(100)
                client.send(x[4])
                client.recv(100)
                client.send(str(x[0]))
                client.recv(100)
                offset = str(offset)
                client.send(offset)
                newsize = client.recv(1024)
                x[2] = int(newsize)
                break
        files[filename] = file
        client.send('done')
        client.recv(100)
        menu(client)

def end(client, filename):
    file = files[filename]
    last = file[-1]
    client.send(last[1])
    client.recv(100)
    client.send(last[3])
    client.recv(100)
    client.send(last[4])
    client.recv(100)
    client.send(str(last[0]))
    newsize = client.recv(1024)
    newsize = int(newsize)
    last[2] = newsize
    file[-1] = last
    files[filename] = file
    menu(client)

def createfile(client):
    client.send("Enter name of file you want to create")
    filename = client.recv(1024)
    if filename not in files:
        client.send('SEND')
        chunk = 0
        filechunks = []
        string = client.recv(1024)
        while string == 'send chunk server':
            chunk = chunk + 1
            sock = random.choice(chunk_servers_addr)
            copy1 = random.choice(chunk_servers_addr)
            while copy1 == sock:
                copy1 = random.choice(chunk_servers_addr)
            
            copy2 = random.choice(chunk_servers_addr)
            while copy2 == sock or copy2 == copy1:
                copy2 = random.choice(chunk_servers_addr)

            client.send(sock)
            client.recv(100)
            client.send(copy1)
            client.recv(100)
            client.send(copy2)
            client.recv(100)
            client.send('ok')
            size = client.recv(1024)
            size = int(size)
            c = [chunk, sock, size, copy1, copy2]
            filechunks.append(c)
            client.send('ok')
            string = client.recv(1024)
        if(string == 'FINISHED'):
            files[filename] = filechunks
            print files
            client.send('DONE')
            client.recv(80)
            menu(client)
    else:
        client.send('filename already exists')
        client.recv(80)
        menu(client)


def Main():
    ip = '127.0.0.1'
    port = 7000
    s = socket.socket()
    s.bind((ip, port))
    s.listen(10) # max number of connections
    port = 50184

    # x = socket.socket()
    # x.connect((ip, port))
    # print 'connected to server'

    while True:
        client, addr = s.accept()
        thread.start_new_thread(identify,(client,addr))

if __name__=="__main__":
    Main()