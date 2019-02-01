import socket
import thread
import os

def menu(s):
    response = ''
    while (response != 'read' and response != 'write'):
        string = s.recv(1024)
        response = raw_input(string + '\n')
        s.send(response)
    
    if(response == 'read'):
        readfunc(s)
    else:
        writefunc(s)


def readfunc(s):
    string = s.recv(1024)
    filename = raw_input(string + '\n')
    s.send(filename)
    string = s.recv(1024)
    corrupted = False
    data = ''
    corruptedcchunk = []
    if(string == 'starting read'):
        while (string != 'FINISHED'):
            s.send('send chunk server')
            sock = s.recv(1024)
            s.send('send chunk number')
            chunk = s.recv(1024)
            chunk = int(chunk)
            x = socket.socket()
            ip = '127.0.0.1'
            x.connect((ip, int(sock)))
            x.send('read')
            x.recv(50)
            x.send(filename)
            x.recv(50)
            x.send(str(chunk))
            st = x.recv(1024)
            if(st == 'sending data'):
                x.send('ok')
                while st != 'FINISHED':
                    data = data + x.recv(64000)
                    x.send('finished?')
                    st = x.recv(100)
                    if(st == 'sending data'):
                        x.send('ok')
                    else:
                        break
            elif(st == 'data corrupted'):
                corrupted = True
                x.send('send chunk')
                corruptedcchunk.append(x.recv(1024))
            x.close()
            s.send('finished?')
            string = s.recv(100)
        if(corrupted == True):
            print 'data corrupted'
            print 'corrupted chunk(s): ', corruptedcchunk
        else:
            print data
            f = open('copy.txt', 'w')
            f.write(data)
            f.close()
        s.send('ok')
    else:
        print string
        s.send('ok')
    menu(s)


def writefunc(s):
    string = s.recv(1024)
    response = raw_input(string + '\n')
    s.send(response)
    response = int(response)
    if(response == 1):
        existing(s)
    else:
        creatfile(s)

def existing(s):
    string = s.recv(1024)
    filename = raw_input(string + '\n')
    s.send(filename)
    st = s.recv(100)
    if st == 'file does not exist':
        print st
        s.send('ok')
        menu(s)
    else:
        choice = raw_input(st + '\n')
        s.send(choice)
        choice = int(choice)
        if choice == 1:
            middle(s, filename)
        else:
            end(s, filename)

def middle(s, filename):
    st = s.recv(100)
    offset = raw_input(st + '\n')
    x = int(offset)
    while x < 0:
        print 'invalid input'
        offset = raw_input(st + '\n')
        x = int(offset)

    s.send(offset)
    port = s.recv(1024)
    port = int(port)

    s.send('send first copy')
    copyone = s.recv(1024)
    s.send('send second copy')
    copytwo = s.recv(1024)
    x= socket.socket()
    x.connect(('127.0.0.1', port))
    s.send('send chunk number')
    chunk = s.recv(1024)
    s.send('new offset')
    offset = s.recv(1024)
    x.send('write middle')
    x.recv(1024)
    x.send(filename)
    x.recv(1024)
    x.send(chunk)
    x.recv(100)
    x.send(offset)
    st = x.recv(100)
    data = raw_input(st + '\n')
    x.send(data)
    newsize = x.recv(1024)
    x.close()

    copy1 = socket.socket()
    copy1.connect(('127.0.0.1', int(copyone)))
    copy1.send('write copy middle')
    copy1.recv(1024)
    copy1.send(filename)
    copy1.recv(1024)
    copy1.send(chunk)
    copy1.recv(100)
    copy1.send(offset)
    copy1.recv(100)
    copy1.send(data)

    copy2 = socket.socket()
    copy2.connect(('127.0.0.1', int(copytwo)))
    copy2.send('write copy middle')
    copy2.recv(1024)
    copy2.send(filename)
    copy2.recv(1024)
    copy2.send(chunk)
    copy2.recv(100)
    copy2.send(offset)
    copy2.recv(100)
    copy2.send(data)

    s.send(newsize)
    st = s.recv(100)
    print st
    s.send('ok')
    menu(s)

def end(s, filename):
    port = s.recv(1024)
    s.send('send copy 1')
    copyone = s.recv(1024)
    s.send('send copy 2')
    copytwo = s.recv(1024)
    s.send('ok')
    chunk = s.recv(1024)
    x = socket.socket()
    x.connect(('127.0.0.1',int(port)))
    x.send('write end')
    x.recv(100)
    x.send(filename)
    st = x.recv(100)
    data = raw_input(st + '\n')
    x.send(data)
    newsize = x.recv(1024)
    x.close()

    copy1 = socket.socket()
    copy1.connect(('127.0.0.1', int(copyone)))
    copy1.send('write copy end')
    copy1.recv(100)
    copy1.send(filename)
    copy1.recv(100)
    copy1.send(data)

    copy2 = socket.socket()
    copy2.connect(('127.0.0.1', int(copytwo)))
    copy2.send('write copy end')
    copy2.recv(100)
    copy2.send(filename)
    copy2.recv(100)
    copy2.send(data)

    s.send(newsize)
    menu(s)

def creatfile(s):   
    string = s.recv(1024)
    filename = raw_input(string + '\n')
    s.send(filename)
    resp = s.recv(1024)
    if(resp == 'SEND'):
        f = open(filename,'r')
        l = f.read(64000)
        chunk = 1
        while (l):
            s.send('send chunk server')
            sock = s.recv(1024)
            s.send('send copy1')
            copyone = s.recv(1024)
            s.send('send copy2')
            copytwo = s.recv(1024)
            x = socket.socket()
            ip = '127.0.0.1'
            x.connect((ip, int(sock)))
            x.send('create')
            x.recv(50)
            x.send(filename)
            x.recv(50)
            x.send(str(chunk))
            x.recv(50)
            x.send(l)
            size = x.recv(1024)
            x.close()

            copy1 = socket.socket()
            copy1.connect((ip, int(copyone)))
            copy1.send('copy')
            copy1.recv(50)
            copy1.send(filename)
            copy1.recv(50)
            copy1.send(str(chunk))
            copy1.recv(50)
            copy1.send(l)
            copy1.close()
            
            copy2 = socket.socket()
            copy2.connect((ip, int(copytwo)))
            copy2.send('copy')
            copy2.recv(50)
            copy2.send(filename)
            copy2.recv(50)
            copy2.send(str(chunk))
            copy2.recv(50)
            copy2.send(l)
            copy2.close()

            s.send('sending file size')
            s.recv(100)
            s.send(size)
            s.recv(100)
            l = f.read(64000)
            chunk = chunk + 1
        f.close()
        s.send('FINISHED')
        if(s.recv(1024) == 'DONE'):
            s.send('ok')
            print 'DONE'
            menu(s)
    else:
        print resp
        s.send('ok')
        menu(s)

def Main():
    ip = '127.0.0.1'
    port = 7000
    s = socket.socket()
    s.connect((ip,port))
    print 'connected'
    s.recv(1024)
    s.send("client")
    menu(s)
if __name__=="__main__":
    Main()