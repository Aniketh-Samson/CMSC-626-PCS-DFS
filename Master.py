from http import server
from multiprocessing import Lock
from operator import contains
import socket
import os
from _thread import *
import json
import time

from nbformat import read
import encryptdecrypt

#Host = '127.0.0.3'
Host = '0.0.0.0'
StorageServer = '127.0.0.3'
Port = 12344

Storageport_A = {12348,12350,12347}
Storageport_B = {}

Stroageserverpc = ['10.0.0.71']

ThreadCount = 0

ServerSocket = socket.socket()

#golbal vars
ClientSockets = []
A_records = []
B_records = []
Availableservers = []
founditin = ""
gotthedata  = False
needed = [0,0,0,0]
refreshedservers = []

admin_encrypt = encryptdecrypt.Encrypt_Decrypt("password")

#Check the credentials of the admin
admin_password = input("Admin Password :- ")

with open("data.txt",'r') as fp:
    content = fp.read()

if not admin_encrypt.encrypt(admin_password).decode('utf-8') == content:
    print("Wrong admin credentials")
    exit()


#mutex locks for the files present
A_mutex = []
B_mutex = []

try:
    ServerSocket.bind((Host,Port))
except socket.error as e:
    print(str(e))

print('Listening for connections')
ServerSocket.listen(5)

System_encrypt = encryptdecrypt.Encrypt_Decrypt(admin_password)


def threaded_client(connection):
    connection.send(str.encode('Server welcomes you'))
    while True:
        #first receive the authentication 
        users_authentication_data = {}
        try:
            with open('json_data.json') as json_file:
                users_authentication_data = json.load(json_file)
        except:
            pass
        auth_data_username = connection.recv(2048)
        print("Username",auth_data_username)
        auth_data_username = System_encrypt.encrypt(auth_data_username.decode('utf-8')).decode('utf-8')
        
        response_username = ""
        #check if the user is an existing one or not
        if auth_data_username in users_authentication_data:
            response_username = "Pswd"
        else:
            response_username = "New"
        
        connection.sendall(str.encode(response_username))

        #receive the encrypted password and either save or validate

        password = connection.recv(2048).decode('utf-8')
        isValid = "Valid"

        if response_username == "Pswd" :
            print("Existing User - so validating the password")
            if str(password) != str(users_authentication_data[auth_data_username]):
                print("User found but incorrect password")
                print("Looking for",users_authentication_data[auth_data_username])
                print("found",password)
                print("found",type(password),type(users_authentication_data[auth_data_username]))
                isValid = "Not Valid"
        else:
            print("New User")
            users_authentication_data[auth_data_username] = str(password)
            with open('json_data.json', 'w') as outfile:
                json.dump(users_authentication_data, outfile)
        
        connection.sendall(str.encode(isValid))

        #the further operations
        data = connection.recv(2048)
        data = data.decode('utf-8')
        print("Command from the client",data)
        if "grant" in data:
            #its a grant permission operation
            data_break = data.split()
            data_break[3] = System_encrypt.encrypt(data_break[3]).decode('utf-8')
            data = ""
            for i in data_break:
                data += i
                data += " "
            print("After grant permission parsing",data)        
       
        inputdata = auth_data_username + " "
        inputdata += data
        reply = 'Main Server says got the following input from you '+data
        print('Got the following data from the server'+data)
        if not data:
            break
        #check existing records to pinpoint the server
        data = data.split()

        needed = [0,0,0,0]
        #now determine in which server the file is located
        needed,file_index = checktherecord(data[1])
        
        replies = ""
        finalmessage = "commit"

        #establish the connection with the storage servers
        refreshedservers.clear()
        finalmessage , replies = connectwithstorages(inputdata,needed,data,refreshedservers,replies,file_index,finalmessage,auth_data_username)

        connection.sendall(str.encode(reply))
        print("Replies 0",replies)
        connection.sendall(str.encode(replies))
    connection.close()


def closethesockets(ClientSockets):
    for socket in ClientSockets:
        socket.close()

#check if the file is already present at the records and can we map it to any of the servers
def checktherecord(filename):
    needed = [0,0,0,0]
    A_records.append(filename)
    index_at = A_records.index(filename)
    mutex = Lock()
    A_mutex.append(mutex)
    needed[0] = 1
    needed[1] = 1
    needed[2] = 1
    needed[3] = 1
    return needed,index_at
    
#establish connections with storage servers and see if we can find anything
#if we are not able to connect with all the four then return/print an error message
def connectwithstorages(inputdata,needed,data,servers,replies,file_index,finalmessage,auth_data_username):
    print("Connectwithstorages {} {} {}".format(data,servers,needed))
    index = -1
    print("Here {} {} ".format(len(Storageport_A),Storageport_A))
    for i in Storageport_A:
        print("Here in the loop for",i)
        CS = socket.socket()
        if i == 12345 :
                    if needed[index] == 1:
                        try:
                            CS.connect((StorageServer, i))
                            servers.append(CS)
                            index+=1
                            Response = CS.recv(1024)
                            print(Response.decode('utf-8'))
                            parsethedata(data,servers,inputdata,index)
                        except socket.error as e:
                            print('Unable to connect - ',i)
                            finalmessage = "abort"
        else:
            if needed[index] == 1:
                try:    
                            print("trying to connect to ",StorageServer[0],i)
                            #CS.connect((Stroageserverpc[0], i))
                            CS.connect((StorageServer, i))                        
                            servers.append(CS)
                            index+=1
                            Response = CS.recv(1024)
                            print(Response.decode('utf-8'))
                            parsethedata(data,servers,inputdata,index)
                except socket.error as e:                        
                            print('Unable to connect - ',i)
                            finalmessage = "abort"
    
    print("Final message ",finalmessage)
    print('Data===========',data)

    if (finalmessage == "commit" and data[0]=="create"):
        print("commited in writing file")
        print('------------------In create function\n')
        var=[]
        var=data[1].split("/")
        length=len(var)
        print("Var",var,"length",length)
        f1 = open('filelist.txt', "a+")
        f1.write(var[length-1])
        print("Var",var)
        f1.write("\n")
        f1.close()
        print("\ndata is here",data)
        print("/n there to look file updated")
    elif (finalmessage == "commit" and data[0]=="delete"):
        print('------------------In Delete function\n')
        with open("filelist.txt", 'r') as file:
            lines = file.readlines()
        with open("filelist.txt", 'w') as file:
            for line in lines:
                # readlines() includes a newline character
                if line.strip("\n") != data[1]:
                    file.write(line)
    elif(finalmessage == "commit" and data[0] =="rename"):
        print('------------------In Delete function\n')
        with open("filelist.txt", 'r') as file:
            lines = file.readlines()
        with open("filelist.txt", 'w') as file:
            for line in lines:
                # readlines() includes a newline character
                if line.strip("\n") != data[1]:
                    file.write(line)
        print("commited in writing file")
        print('------------------In create function\n')
        f1 = open('filelist.txt', "a+")
        f1.write(data[2])
        #print("Var",var)
        f1.write("\n")
        f1.close()
        print("\ndata is here",data)
        print("/n there to look file updated")

    replies = sendthefinalmessage(finalmessage,data,servers,file_index)
    #print("Replies here",replies[index])
    return finalmessage,replies


#send the command and recieve the prepare message from the storage servers
def parsethedata(data,servers,inputdata,index):
    print("Inside the parsethedata {} {} {}".format(data,servers,inputdata))
    servers[index].send(str.encode(inputdata))
    prepare_response = servers[index].recv(1024)
      
    print("Got the following prepare response from the storage server",prepare_response.decode('utf-8'))


def sendthefinalmessage(finalmessage ,data,servers,file_index):
    print("Inside the sendthefinal message {} {} {} {}".format(servers,data,file_index,finalmessage))
    readcontent = ""

    for i in range(len(servers)):
        if(data[0] == "read"):            
                    try:
                        A_mutex[file_index].acquire()
                        servers[i].send(str.encode(finalmessage))
                        filedata = servers[i].recv(2048)
                        print('File contents received -' ,filedata.decode('utf-8'))
                        readcontent = filedata.decode('utf-8')
                    finally:
                        A_mutex[file_index].release()
        
        elif (data[0] == "create"):
                    print("Calling the create function",i)
                    servers[i].send(str.encode(finalmessage))
                    Response2 = servers[i].recv(2048)
                    print(Response2.decode('utf-8'))
                    print("Read content")
                    readcontent = Response2.decode('utf-8')

        elif (data[0] == "rename"):
                    try:
                        A_mutex[file_index].acquire()
                        servers[i].send(str.encode(finalmessage))
                        Response2 = servers[i].recv(2048)
                        print("Response from the storage server",Response2.decode('utf-8'))
                        readcontent = Response2.decode('utf-8')
                    finally:
                        A_mutex[file_index].release()
        
        elif(data[0] == "delete"):
                try:
                    A_mutex[file_index].acquire()
                    servers[i].send(str.encode(finalmessage))
                    Response2 = servers[i].recv(2048)
                    print("Response from the storage server",Response2.decode('utf-8'))
                    readcontent = Response2.decode('utf-8')
                finally:
                    A_mutex[file_index].release()

        elif(data[0] == "write"):                
                try:
                    A_mutex[file_index].acquire()
                    servers[i].send(str.encode(finalmessage))
                    Response2 = servers[i].recv(2048)
                    print("Response from the storage server",Response2.decode('utf-8'))
                    readcontent = Response2.decode('utf-8')
                finally:
                    A_mutex[file_index].release()

        elif(data[0] == "grant"):                
                #try:
                    #A_mutex[file_index].acquire()
                    servers[i].send(str.encode(finalmessage))
                    Response2 = servers[i].recv(2048)
                    print("Response from the storage server",Response2.decode('utf-8'))
                    readcontent = Response2.decode('utf-8')
                # finally:
                #     A_mutex[file_index].release()

        elif (data[0]=='list'):

            servers[i].send(str.encode(finalmessage))
            dir_list = servers[i].recv(2048)
            print("Response from the storage server", dir_list)
            # if i==0:
            #     readcontent += "Files in Server"+str(i)+": "+dir_list.decode('utf-8')
            # else:
            #     readcontent += "|Files in Server" + str(i)+": "+dir_list.decode('utf-8')

            readcontent = dir_list.decode('utf-8')

    for sockets in range(0,len(servers)):
            print(type(servers[sockets]))
            servers[sockets].close()
    
    print("Read content",readcontent)

    return readcontent

while True:
    Client,addr = ServerSocket.accept()
    print('Connected to: '+addr[0]+':'+str(addr[1]))
    start_new_thread(threaded_client,(Client,))
    ThreadCount+=1

ServerSocket.close()


# what if needed is B and B is down - ? How do we handle that scenario
# create a queue for each server and once when the server establishes a connection with the main server , we can just keep sending the data