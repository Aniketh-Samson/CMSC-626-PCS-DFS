import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random
from multiprocessing import Lock
import socket
import json
from _thread import *
import Encrypt

BLOCK_SIZE = 32
password = "test_password"

# Host = '127.0.0.3'
Host = '0.0.0.0'
StorageServer = 'localhost'
Port = 3123
# Aniketh_IP='10.200.154.209'

Stroageserverpc = ['localhost']
Storageport_A = [4321, 4322, 4323]
Storageport_B = {89898, 12331}

E2 = Encrypt.Encrypt_Decrypt("test_password")
encrypted = E2.encrypt("This is a secret message")
#print(encrypted)

from http import server

ThreadCount = 0

ServerSocket = socket.socket()

# golbal vars
ClientSockets = []
A_records = []
B_records = []
Availableservers = []
founditin = ""
gotthedata = False
needed = [0, 0, 0, 0]
refreshedservers = []

# mutex locks for the files present
A_mutex = []
B_mutex = []

try:
    ServerSocket.bind((Host, Port))
except socket.error as e:
    print(str(e))

print('Listening for connections')
ServerSocket.listen(5)


def threaded_client(connection):
    connection.send(str.encode('Server welcomes you'))
    while True:
        #######################adding master user authentication code
        # first receive the authentication
        users_authentication_data = {}
        try:
            with open('json_data.json') as json_file:
                users_authentication_data = json.load(json_file)
        except:
            pass
        auth_data = connection.recv(2048)
        print("Here", auth_data)
        auth_data = auth_data.decode('utf-8')
        auth_data = auth_data.split()
        print("auth data split", auth_data)
        username = auth_data[0]
        print(username,'Username')
        password = auth_data[1]
        print(password, 'password')
        isValid = "Valid"
        if auth_data[0] in users_authentication_data:
            print("user found")
            if str(password) != str(users_authentication_data[username]):
                print("User found but incorrect password")
                print("Looking for", users_authentication_data[username])
                print("found", password)
                print("found", type(password), type(users_authentication_data[username]))
                isValid = "Not Valid"
        else:
            print("New User")
            users_authentication_data[username] = str(password)
            with open('json_data.json', 'w') as outfile:
                json.dump(users_authentication_data, outfile)

        connection.sendall(str.encode(isValid))
        ########################

        data = connection.recv(2048)
        # data = data.decode('utf-8')
        data = E2.decrypt(data)
        data = bytes.decode(data)
        print(data)
        inputdata = data
        reply = 'Main Server says got the following input from you ' + data
        print('Got the following data from the server' + data)
        if not data:
            break
        # check existing records to pinpoint the server
        data = data.split()

        needed = [0, 0, 0, 0]
        # now determine in which server the file is located
        if len(data) == 1:
            data.append("abc")

        needed, file_index = checktherecord(data[1])

        replies = ''
        finalmessage = "commit"

        # establish the connection with the storage servers
        refreshedservers.clear()
        finalmessage, replies = connectwithstorages(inputdata, needed, data, refreshedservers, replies, file_index,
                                                    finalmessage)

        connection.sendall(str.encode(reply))
        connection.sendall(str.encode(replies))
    connection.close()


def closethesockets(ClientSockets):
    for socket in ClientSockets:
        socket.close()


# check if the file is already present at the records and can we map it to any of the servers
def checktherecord(filename):
    needed = [0, 0, 0, 0]
    A_records.append(filename)
    index_at = A_records.index(filename)
    mutex = Lock()
    A_mutex.append(mutex)
    needed[0] = 1
    needed[1] = 1
    needed[2] = 1
    needed[3] = 1
    return needed, index_at


# establish connections with storage servers and see if we can find anything
# if we are not able to connect with all the four then return/print an error message
def connectwithstorages(inputdata, needed, data, servers, replies, file_index, finalmessage):
    print("Connectwithstorages {} {} {}".format(data, servers, needed))
    index = -1

    print(replies)

    print("Here {} {} ".format(len(Storageport_A), Storageport_A))
    for i in Storageport_A:
        print("Here in the loop for", i)
        CS = socket.socket()
        if i == Storageport_A[0]:
            if needed[index] == 1:
                try:
                    CS.connect((StorageServer, i))
                    servers.append(CS)
                    index += 1
                    Response = CS.recv(1024)
                    print(Response.decode('utf-8'))
                    parsethedata(data, servers, inputdata, index)
                except socket.error as e:
                    print('Unable to connect - ', i)
                    finalmessage = "abort"
            elif needed[index] == 1:
                parsethedata(data, servers, inputdata, index)
        elif i == Storageport_A[1]:
            if needed[index] == 1:
                try:
                    # print("trying to connect to ", StorageServer[0], i)
                    # CS.connect((Stroageserverpc[0], i))
                    CS.connect((StorageServer, i))
                    servers.append(CS)
                    index += 1
                    Response = CS.recv(1024)
                    print(Response.decode('utf-8'))
                    parsethedata(data, servers, inputdata, index)
                except socket.error as e:
                    print('Unable to connect - ', i)
                    finalmessage = "abort"
            elif needed[index] == 1:
                parsethedata(data, servers, inputdata, index, file_index)

        elif i == Storageport_A[2]:
            if needed[index] == 1:
                try:
                    # print("trying to connect to ", StorageServer[0], i)
                    # CS.connect((Stroageserverpc[0], i))
                    CS.connect((StorageServer, i))
                    servers.append(CS)
                    index += 1
                    Response = CS.recv(1024)
                    print(Response.decode('utf-8'))
                    parsethedata(data, servers, inputdata, index)
                except socket.error as e:
                    print('Unable to connect - ', i)
                    finalmessage = "abort"
            elif needed[index] == 1:
                parsethedata(data, servers, inputdata, index, file_index)

    print("Final message ", finalmessage)
    replies = sendthefinalmessage(finalmessage, data, servers, file_index)
    return finalmessage, replies


# send the command and recieve the prepare message from the storage servers
def parsethedata(data, servers, inputdata, index):
    print("Inside the parsethedata {} {} {}".format(data, servers, inputdata))
    servers[index].send(str.encode(inputdata))
    prepare_response = servers[index].recv(1024)

    print("Got the following prepare response from the storage server", prepare_response.decode('utf-8'))


def sendthefinalmessage(finalmessage, data, servers, file_index):
    print("Inside the sendthefinal message {} {} {} {}".format(servers, data, file_index, finalmessage))
    readcontent = ""
    for i in range(len(servers)):
        if (data[0] == "read"):
            try:
                A_mutex[file_index].acquire()
                servers[i].send(str.encode(finalmessage))
                filedata = servers[i].recv(2048)
                print('File contents received -', filedata.decode('utf-8'))
                readcontent = filedata.decode('utf-8')
            finally:
                A_mutex[file_index].release()

        elif (data[0] == "create"):
            print("Calling the create function", i)
            servers[i].send(str.encode(finalmessage))
            Response2 = servers[i].recv(2048)
            print(Response2.decode('utf-8'))
            readcontent = Response2.decode('utf-8')

        elif (data[0] == "rename"):
            servers[i].send(str.encode(finalmessage))
            Response2 = servers[i].recv(2048)
            print("Response from the storage server", Response2.decode('utf-8'))
            readcontent = Response2.decode('utf-8')

        elif (data[0] == "delete"):
            servers[i].send(str.encode(finalmessage))
            Response2 = servers[i].recv(2048)
            print("Response from the storage server", Response2.decode('utf-8'))
            readcontent = Response2.decode('utf-8')

        elif (data[0] == "write"):
            try:
                A_mutex[file_index].acquire()
                servers[i].send(str.encode(finalmessage))
                Response2 = servers[i].recv(2048)
                print("Response from the storage server", Response2.decode('utf-8'))
                readcontent = Response2.decode('utf-8')
            finally:
                A_mutex[file_index].release()
        elif (data[0]=='list'):

            servers[i].send(str.encode(finalmessage))
            dir_list = servers[i].recv(2048)
            print("Response from the storage server", dir_list)
            if i==0:
                readcontent += "Files in Server"+str(i)+": "+dir_list.decode('utf-8')
            else:
                readcontent += "|Files in Server" + str(i)+": "+dir_list.decode('utf-8')


    for sockets in range(0, len(servers)):
        print(type(servers[sockets]))
        servers[sockets].close()

    return readcontent


while True:
    Client, addr = ServerSocket.accept()
    print('Connected to: ' + addr[0] + ':' + str(addr[1]))
    start_new_thread(threaded_client, (Client,))
    ThreadCount += 1

ServerSocket.close()

# what if needed is B and B is down - ? How do we handle that scenario
# create a queue for each server and once when the server establishes a connection with the main server , we can just keep sending the data
