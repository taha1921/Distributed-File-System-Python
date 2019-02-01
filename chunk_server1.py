import socket
import thread
import os
import hashlib

chunks = dict()
copies = {}

def action(client, addr):
    string = client.recv(1024)
    if(string == 'create'):
        client.send('filename')
        filename = client.recv(1024)
        client.send('chunknumber')
        chunk = client.recv(1024)
        chunk = int(chunk)
        client.send('ok')
        createchunk(client, filename, chunk)
    
    if(string == 'read'):
        read(client)

    if(string == 'write middle'):
        writemiddle(client)

    if(string == 'write end'):
        writeend(client)

    if(string == 'copy'):
        copy(client)

    if(string == 'write copy middle'):
        copymiddle(client)

    if(string == 'write copy end'):
        copyend(client)

    return

def copyend(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('what data do you want to write')
    data = client.recv(1024)
    metadata = copies[filename]
    lastchunk = metadata[-1]
    chunk = lastchunk[1]
    file = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(file, 'a')
    f.write(data)
    f.close()
    f = open(file, 'r')
    d = os.path.getsize(file)
    data = f.read(d)
    f.close()
    h = hashlib.sha1(data).hexdigest()
    lastchunk[0] = h
    metadata[-1] = lastchunk
    copies[filename] = metadata



def copymiddle(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('send chunk')
    chunk = client.recv(1024)
    client.send('send offset')
    offset = client.recv(1024)
    offset = int(offset)
    client.send('what data do you want to write')
    data = client.recv(1024)
    file = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(file, 'r')
    l = offset-1
    x = f.read(l)
    x = x + data
    d = os.path.getsize(file)
    remaining = f.read(d)
    x = x + remaining
    f.close
    f = open(file, 'w')
    f.seek(0)
    f.write(x)
    f.close()
    f = open(file, 'r')
    d = os.path.getsize(file)
    data = f.read(d)
    f.close()
    h = hashlib.sha1(data).hexdigest()
    metadata = copies[filename]
    for x in metadata:
        if(x[1] == int(chunk)):
            x[0] = h
            break
    copies[filename] = metadata

def writeend(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('what data do you want to write')
    data = client.recv(1024)
    metadata = chunks[filename]
    lastchunk = metadata[-1]
    chunk = lastchunk[1]
    file = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(file, 'a')
    f.write(data)
    f.close()
    f = open(file, 'r')
    d = os.path.getsize(file)
    data = f.read(d)
    f.close()
    h = hashlib.sha1(data).hexdigest()
    lastchunk[0] = h
    metadata[-1] = lastchunk
    chunks[filename] = metadata
    d = str(d)
    client.send(d)

def writemiddle(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('send chunk')
    chunk = client.recv(1024)
    client.send('send offset')
    offset = client.recv(1024)
    offset = int(offset)
    client.send('what data do you want to write')
    data = client.recv(1024)
    file = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(file, 'r')
    l = 0
    if offset == 0:
        l = 0
    else:
        l = offset-1
    x = f.read(l)
    x = x + data
    d = os.path.getsize(file)
    remaining = f.read(d)
    x = x + remaining
    f.close
    f = open(file, 'w')
    f.seek(0)
    f.write(x)
    f.close()
    f = open(file, 'r')
    d = os.path.getsize(file)
    data = f.read(d)
    f.close()
    h = hashlib.sha1(data).hexdigest()
    metadata = chunks[filename]
    for x in metadata:
        if(x[1] == int(chunk)):
            x[0] = h
            break
    chunks[filename] = metadata
    d = str(d)
    client.send(d)

def read(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('chunknumber')
    chunk = client.recv(1024)
    chunk = int(chunk)
    openfile = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(openfile, 'r')
    k = os.path.getsize(openfile)
    data = f.read(k)
    f.close()
    h = hashlib.sha1(data).hexdigest()
    f = open(openfile, 'r')
    data = f.read(64000)
    metadata = chunks[filename]
    for x in metadata:
        if(x[1] == chunk):
            if(x[0] == h):
                client.send('sending data')
                client.recv(10)
                while (data):
                    client.send(data)
                    data = f.read(64000)
                    client.recv(100)
                    if(data):
                        client.send('sending data')
                        client.recv(100)
                    else:
                        client.send('FINISHED')
                        break
                f.close()
                break
            else:
                client.send('data corrupted')
                client.recv(100)
                client.send(openfile)
                break


def createchunk(client, filename, chunk):
    data = client.recv(64000)
    metadata = [hashlib.sha1(data).hexdigest(), chunk]
    if(chunks.has_key(filename)):
        chunks[filename].append(metadata)
    else:
        chunks[filename] = [metadata]
    newfile = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(newfile, "w")
    f.write(data)
    f.flush()
    os.fsync(f.fileno())
    f.close()
    size = os.path.getsize(newfile)
    client.send(str(size))
    return

def copy(client):
    client.send('filename')
    filename = client.recv(1024)
    client.send('chunknumber')
    chunk = client.recv(1024)
    chunk = int(chunk)
    client.send('ok')
    data = client.recv(64000)
    metadata = [hashlib.sha1(data).hexdigest(), chunk]
    if(copies.has_key(filename)):
        copies[filename].append(metadata)
    else:
        copies[filename] = [metadata]
    newfile = filename[:-4] + '_chunk' + str(chunk) + '.txt'
    f = open(newfile, "w")
    f.write(data)
    f.close()

def main():
    ip = '127.0.0.1'
    port = 7000
    s = socket.socket()
    s.connect((ip,port))
    s.recv(1024)
    s.send("chunk_server")
    port = s.recv(1024)
    port = int(port)
    print 'connected'

    x = socket.socket()
    x.bind((ip, port))
    x.listen(10)

    while True:
        client, addr = x.accept()
        thread.start_new_thread(action,(client,addr))



if __name__ == '__main__':
    main()