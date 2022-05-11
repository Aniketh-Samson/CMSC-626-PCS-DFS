
from ast import In
from operator import contains
from os import rename
import socket
from typing import final
from urllib import response

import encryptdecrypt

ClientSocket = socket.socket()
host = '10.0.0.197'
#host = '10.200.77.6'
port = 12344

def checktheargs(Input):
    Input = Input.split()
    if(Input[0] ==  "create" and len(Input) <2):
        print("Please pass the file name that you want to create like- create hello.txt")
        return False
    elif(Input[0] ==  "rename" and len(Input) <2):
        print("Please pass the file name that you want to rename like- rename hello.txt hello2.txt")
        return False
    if(Input[0] ==  "delete" and len(Input) <2):
        print("Please pass the file name that you want to delete like- delete hello.txt")
        return False
    if(Input[0] ==  "write" and len(Input) <3):
        print("Please pass the file name  and that you want to write like- write hello.txt how you doing?")
        return False
    return True

print('Waiting for connection')
try:
    ClientSocket.connect((host, port))
except socket.error as e:
    print("----------------------")
    print(str(e))

Response = ClientSocket.recv(1024)

#Encrypt common
E_common = encryptdecrypt.Encrypt_Decrypt("commonfiles")

while True:

    username = input("Enter your username -: ")
    #send the username to check if the user is an existing one or a new one
    ClientSocket.send(str.encode(username))

    #response from the server on the presence of the user
    response_username = ClientSocket.recv(1024).decode('utf-8')
    if(response_username == "Pswd"):
        #existing user
        password = input("Enter your password -: ")
    else:
        password = input("Please set a passoword for your profile -: ")


    #encrypt the password before sending it
    E1 = encryptdecrypt.Encrypt_Decrypt(username)
    #username = E1.encrypt(username)
    password = E1.encrypt(password)
    authenticationDetails = str(username)+" "+str(password)
    #print("Sending the following",password)
    ClientSocket.send(str.encode(authenticationDetails))

    #if user is authenticated proceed, else close the connection

    isValid = ClientSocket.recv(1024)
    #print("IsValid",isValid)
    if isValid.decode('utf-8') != "Valid":
        print("Please enter correct crendentials")
        break
    print('Operations Sample Input')
    print('1: Create path/filename.txt')
    print('2: write path/filename.txt text_to_input')
    print('3: read path/filename.txt')
    print('4: delete path/filename.txt')
    print("5: grant path/filename.txt toUser permission_type \n")

    Input = input('Please enter the command :- ')
    if (not checktheargs(Input)):
        Input = input('Please enter the command :- ')
    #split the input to know the operation requested by the user
    #and wait for the responses accordingly

    Input_split = Input.split()
    
    inputInEncrypt = Input_split[0]
    inputInEncrypt += " "
    folder_path = []
    folder_path = Input_split[1].split("/")
    #print("Folder path",folder_path,len(folder_path))

    #format the path based on the given path by user

    if not Input_split[0] == "list":
        if len(folder_path) == 1:
            finalFolderPath = "root"
            finalFolderPath += "/"
            inputInEncrypt += finalFolderPath
            inputInEncrypt += str(E1.encrypt(folder_path[0][-3]).decode('utf-8'))
            inputInEncrypt += ".txt"
            inputInEncrypt += " "

        elif folder_path[0] == "shared":
            #split the string based on '/'
            inputInEncrypt += "shared/"
            inputFileName = Input_split[1].split('/')[1]
            inputFileName = inputFileName[:-3]
            inputInEncryptFileName = str(E1.encrypt(inputFileName))[2:]
            inputInEncryptFileName = inputInEncryptFileName[:-1]
            inputInEncrypt += inputInEncryptFileName
            inputInEncrypt += ".txt"
            inputInEncrypt += " "

        else:
            finalFolderPath = ""
            finalFolderPath = folder_path[0]
            finalFolderPath = str(E1.encrypt(finalFolderPath).decode('utf-8'))
            inputInEncrypt += finalFolderPath+"/"
            finalFolderPath = folder_path[1][:-4]
            finalFolderPath = str(E1.encrypt(finalFolderPath).decode('utf-8'))
            inputInEncrypt += finalFolderPath
            inputInEncrypt += ".txt"
            inputInEncrypt += " "

    if(Input_split[0] == "read"):
        #get the data of the file contents
        #print("Sending the following command",inputInEncrypt)
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        Response = Response.decode('utf-8')
        #print(Response)
        filedata = ClientSocket.recv(2048)
        #print("Here is the file data",filedata)
        if(filedata.decode('utf-8') == "No Permission"):
            print("You have no permission to read this file")
        else:

            filedata = filedata.split()
        
            filecontent = ""
            for i in range(0,len(filedata)):
                #print("Filedata",filedata[i])
                filecontent+= E1.decrypt(filedata[i]).decode('utf-8')
                filecontent+= " "
            print('The file contents are :- ',filecontent)
    
    elif(Input_split[0] == "write"):
        #get the data of the file contents
        #print("The file contents are")

        inputInEncryptTextToWrite = ""
        for i in range(2,len(Input_split)):
            inputInEncryptTextToWrite += str(E1.encrypt(Input_split[i]))[2:-1]
            inputInEncryptTextToWrite+= " "
        
        inputInEncrypt += inputInEncryptTextToWrite

        #print("Final Encrypted format ",inputInEncrypt)
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        #print(Response.decode('utf-8'))
        filedata = ClientSocket.recv(2048).decode('utf-8')
        print('Message from the server',filedata)

    elif(Input_split[0] == "create"):
        #know whether the file was created successfully or not
        #print("encrypted input",inputInEncrypt)
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        #print(Response.decode('utf-8'))
        msg = ClientSocket.recv(2048).decode('utf-8').split()
        if msg[0] =="Already" and msg[1] =="present":
            print("File already exists. Please choose a different file name")
        else:
            #print("here",msg[1][:-4])
            print("Message from the server - ",msg[0]+" "+E1.decrypt(msg[1][:-4]).decode('utf-8')+".txt")
    
    elif(Input_split[0] == "rename"):
        #things to be done after the file was renamed
        renameToBe = Input_split[2][:-4]
        print("Rename to be",renameToBe)
        renameToBe = E1.encrypt(renameToBe).decode('utf-8')+".txt"
        inputInEncrypt += renameToBe
        print("Sending the following in",inputInEncrypt)
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        #print(Response.decode('utf-8'))
        msg = ClientSocket.recv(2048)
        print('Message from the server -',msg.decode('utf-8'))

    elif(Input_split[0] == "delete"):
        #things to be done after the file was renamed
        print("Sending the following",inputInEncrypt)
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        #print(Response.decode('utf-8'))
        msg = ClientSocket.recv(2048)
        print('Message from the server -',msg.decode('utf-8'))

    #grantpermission hello.txt new_user read 
    elif(Input_split[0] == "grant"):
        #need  to encrypt the file name with the second user
        
        E_newuser = encryptdecrypt.Encrypt_Decrypt(Input_split[2])
        
        inputInEncryptFileName = str(E_newuser.encrypt(folder_path[1][:-3]).decode('utf-8'))
        inputInEncrypt+=inputInEncryptFileName
        inputInEncrypt += ".txt"
        inputInEncrypt+=" "
        inputInEncrypt += str(Input_split[2]) 
        inputInEncrypt+=" "
        inputInEncrypt += Input_split[3]
        ClientSocket.send(str.encode(inputInEncrypt))
        Response = ClientSocket.recv(1024)
        #print(Response.decode('utf-8'))
        msg = ClientSocket.recv(2048)
        print('Message from the server -',msg.decode('utf-8'))
    
    elif(Input_split[0] == "list"):
          inputInEncrypt = "list files"
          ClientSocket.send(str.encode(inputInEncrypt))
          Response = ClientSocket.recv(1024)
          Response = Response.decode('utf-8')
          Response = ClientSocket.recv(1024).decode('utf-8')
          print("response",Response)
          if(Response == "No Files"):
              print("Currently you have no files created or shared")
          else:
            Response = Response.split()
          #print("Response after the split looks like",Response)
            filecontent = ""
            for i in range(0,len(Response)):
                    filePath = Response[i].split("\\")
                    fileName = ""
                    if len(filePath) == 1:
                        decryptedFileName = Response[i][:-11]
                        print("Decryped File Name",decryptedFileName)
                        decryptedFileName = E1.decrypt(decryptedFileName).decode('utf-8')
                        print("Shared files - ",decryptedFileName+"txt")
                    else:
                        for j in range(1, len(filePath)-1):
                            decryptedFolderPath = E1.decrypt(filePath[j]).decode('utf-8')
                            fileName += decryptedFolderPath+"/"
                        fileName += E1.decrypt(filePath[len(filePath)-1][:-4]).decode('utf-8')+".txt"
                        print('Files - ',fileName)

ClientSocket.close()