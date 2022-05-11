from fileinput import filename
from math import perm
import socket
import os
import threading
from _thread import *
from sre_constants import CATEGORY_NOT_LINEBREAK
from threading import Thread, Lock
import json

from anyio import fail_after
import encryptdecrypt

Host = '0.0.0.0'
Port = 12348
ThreadCount = 0
lock = threading.Lock()
permissions_lock = threading.Lock()
sharedPermission_lock = threading.Lock()

System_encrypt = encryptdecrypt.Encrypt_Decrypt("admin1234")

def Server_copy(conn,addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    conn.send("OK@Welcome to the File Server.".encode("utf-8"))

    while True:
        data = conn.recv(1024)
        if not data:
            break
        print("Got until here",data.decode('utf-8'))
        conn.send(str.encode("Got the data preparing myself to commit"))
        finalmessage = conn.recv(1024)
        finalmessage = finalmessage.decode('utf-8')
        print("The final message I recieved - ",finalmessage)
        if  data and len(data)!=0:
                print('Ready to commit - ', data)
                
                
                #conn.sendall(data)
                data = data.decode('utf-8').split()

                #check for the path if exists okay else create the path
                path = 'C:/Users/nsrag/OneDrive/Desktop/UMBC/SEM2/PCS/Project/Trial1/Multithreaded_Sever/ServerA/'
                permissionsForFile = {}
                if not "shared" in data[2]:
                    path += data[0]+"/"

                permissions_lock.acquire()
                try:
                    with open('permission_data.json') as json_file:
                        permissionsForFile = json.load(json_file)
                except:
                    pass
                finally :
                    permissions_lock.release()

                sharedPermissionsForFile = {}

                sharedPermission_lock.acquire()
                try:
                        with open('sharedPermission_data.json') as json_file:
                            sharedPermissionsForFile = json.load(json_file)
                except:
                        pass
                finally:
                    sharedPermission_lock.release()

                print("Permissions for File",permissionsForFile)

                if(finalmessage == "commit" and data[1] == "create"):

                # Creates a new file
                    folder_path = data[2].split("/")[0]
                    print("Folder path",folder_path)
                    file_name = data[2].split("/")[1]
                    success = ""
                    fileNameWithFolder = data[0]+"/"+folder_path + "/"+ file_name
                    if folder_path == "root":
                        print("Looking for the rooot folder of the user")
                        with open(file_name, 'w') as fp:
                            pass
                    else:
                        path += folder_path
                        if(os.path.exists(path+"/"+file_name) == True):
                            success = "Already present"
                        else:
                            if(os.path.exists(path) == False):
                                os.makedirs(path)
                            with open(path+"/"+file_name, 'w') as fp:
                                pass
                        dir_list = os.listdir(path)
                        print("List of directories and files after creation:")
                        print(dir_list)
                        success = "Created  "+file_name                   
                        permissionsForFile[fileNameWithFolder] = {}
                        permissionsForFile[fileNameWithFolder]['read'] = []
                        permissionsForFile[fileNameWithFolder]['write'] = []
                        permissionsForFile[fileNameWithFolder]['rename'] = []
                        permissionsForFile[fileNameWithFolder]['read'].append(data[0])
                        permissionsForFile[fileNameWithFolder]['write'].append(data[0])
                        permissionsForFile[fileNameWithFolder]['rename'].append(data[0])

                        #acquire the locks and release
                        permissions_lock.acquire()
                        try :
                            with open('permission_data.json', 'w') as outfile:
                                    json.dump(permissionsForFile, outfile)
                        finally:
                            permissions_lock.release()
                    conn.sendall(str.encode(success))

                elif(data[1] == "read"):
                    success = ""

                    if "shared/" in data[2]:
                        print("Looking for shared files")
                        fileNameToLook = data[2][7:]
                        print("Looking for the file",fileNameToLook)

                        if data[0] in sharedPermissionsForFile :
                            print("Found the user in the shared system")
                            sharedFiles = {}
                            sharedFiles = sharedPermissionsForFile[data[0]]
                            print("Shared files",sharedFiles)
                            print
                            if fileNameToLook in sharedFiles:
                                print("User has the file in his/her shared system")
                                actualFileName = sharedFiles[fileNameToLook][0]
                                actualUser = sharedFiles[fileNameToLook][1]
                                #path += actualUser
                                actualUser = System_encrypt.decrypt(actualUser).decode('utf-8')
                                grantedUser = data[0]
                                grantedUser = System_encrypt.decrypt(grantedUser).decode('utf-8')
                                print("Actual User",actualUser,grantedUser)
                                if data[0]  in permissionsForFile[actualFileName]['read']:
                                    with open(path+"/"+actualFileName,'r') as fp:
                                        content = fp.read()
                                    
                                    E1 = encryptdecrypt.Encrypt_Decrypt(actualUser)
                                    E2 = encryptdecrypt.Encrypt_Decrypt(grantedUser)
                                    
                                    finalContent = ""
                                    content = content.split()
                                    for i in range(0,len(content)):

                                        content_temp = str.encode(content[i])
                                        print("Content",content_temp)
                                        content_temp =  E1.decrypt(content_temp).decode('utf-8')
                                        finalContent+= E2.encrypt(content_temp).decode('utf-8')
                                        finalContent += " "

                                    print("Here are the file contents",finalContent)
                                    conn.sendall(str.encode(finalContent))
                                else:
                                    success = "No Permission"
                                    conn.sendall(str.encode(success))

                            else:
                                print("Did not find the file in the shared system of user")
                                conn.sendall(str.encode("No Permission"))
                        else:
                            print("Did not find the user in the shared system")
                            conn.sendall(str.encode("No Permission"))

                    else:
                        folder_path = data[2].split("/")[0]
                        file_name = data[2].split("/")[1]
                        dictionaryLookUp = ""
                        if folder_path == "root":
                            dictionaryLookUp = file_name
                        else:
                            dictionaryLookUp = folder_path+"/"+file_name
                        if data[0] in permissionsForFile[data[0]+"/"+dictionaryLookUp]['read']:
                            
                            if(not folder_path == "root"):
                                path += folder_path
                            with open(path+'/'+file_name,'r') as fp:
                                content = fp.read()
                            print("Here are the file contents",str.encode(content))
                            conn.sendall(str.encode(content))
                        else:
                            success = "No Permission"
                            conn.sendall(str.encode(success))
                
                elif(finalmessage == "commit" and data[1] == "rename"):
                    finalFileName = ""
                    folder_path = data[2].split("/")[0]
                    print(folder_path+"folderpath")
                    if not (folder_path == "root" or folder_path == "shared"):
                        finalFileName = folder_path
                    else:
                        folder_path = ""
                    file_name = data[2].split("/")[1]
                    finalFileName += "/" + file_name
                    to_name = data[3]
                    print("Looking for",data[0]+"/"+finalFileName)
                    print("user",data[0])
                    if data[0]+"/"+finalFileName in permissionsForFile and data[0] in permissionsForFile[data[0]+"/"+finalFileName]['rename']:

                        if os.path.exists(folder_path+"/"+to_name):
                            print("Already exisits")
                            success = "Already exists"
                        else :
                            print(file_name+" file name")
                            count = 0
                            for i in data:
                                count = count+1
                                print(i+" data ")
                            print("Inside the rename operation")
                            success = ""
                            if data[0] in permissionsForFile[data[0] + "/" + finalFileName]['rename']:
                                permissionsForFile[data[0]+"/"+folder_path+"/"+data[3]] = {}
                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['read'] = []
                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['write'] = []
                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['rename'] = []

                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['read'].append(data[0])
                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['write'].append(data[0])
                                permissionsForFile[data[0] + "/" + folder_path + "/" + data[3]]['rename'].append(data[0])
                                permissionsForFile.pop(data[0] + "/" +finalFileName)
                                os.rename(data[0]+"/"+finalFileName,data[0]+"/"+folder_path+"/"+data[3])
                                success = "Renamed to "+data[3]

                                permissions_lock.acquire()
                                try:

                                    with open('permission_data.json', 'w') as outfile:
                                        json.dump(permissionsForFile, outfile)
                                
                                finally :
                                    permissions_lock.release()

                                    # remove the file details even from the shared permission folder
                                for users in sharedPermissionsForFile:
                                    for sharedFiles in sharedPermissionsForFile[users]:
                                        # found the shared file entry
                                        original_filemap = sharedPermissionsForFile[users][sharedFiles]
                                        print("origina filemap",original_filemap)
                                        print("Looking for",data[0]+"/"+data[2])
                                        if len(original_filemap) >=1 and  data[0] + "/" + data[2] == original_filemap[0]:
                                            print("Here for the rename")
                                            sharedPermissionsForFile[users][sharedFiles] = []

                                sharedPermission_lock.acquire()
                                try:
                                    with open('sharedPermission_data.json', 'w') as outfile2:
                                        json.dump(sharedPermissionsForFile, outfile2)
                                finally:
                                    sharedPermission_lock.release()

                                success = "Renamed completely"
                    else:
                        success = "No permission"
                    conn.sendall(str.encode(success))

                elif(finalmessage == "commit" and data[1] == "delete"):
                    success = ""
                    print("Here",data[0]+"/"+data[2])
                    print("Here path",os.path.exists(data[0]+"/"+data[2]))
                    if os.path.exists(data[0]+"/"+data[2]) and data[0]+"/"+data[2] in permissionsForFile and data[0] in permissionsForFile[data[0]+"/"+data[2]]['rename']:

                        #remove the file details even from the shared permission folder
                        for users in sharedPermissionsForFile :
                            for sharedFiles in sharedPermissionsForFile[users]:

                                #found the shared file entry
                                original_filemap = sharedPermissionsForFile[users][sharedFiles]
                                if len(original_filemap) >=1 and data[0]+"/"+data[2] in original_filemap[0]:
                                    sharedPermissionsForFile[users][sharedFiles] = []
                        
                        permissionsForFile.pop(data[0]+"/"+data[2])

                        os.remove(data[0]+"/"+data[2])

                        #dump the python dictionaries into python
                        permissions_lock.acquire()
                        try :
                            with open('permission_data.json', 'w') as outfile:
                                    json.dump(permissionsForFile, outfile)
                        finally:
                            permissions_lock.release()

                        sharedPermission_lock.acquire()
                        try:
                        
                            with open('sharedPermission_data.json', 'w') as outfile2:
                                    json.dump(sharedPermissionsForFile, outfile2)
                        finally:
                            sharedPermission_lock.release()
                        success = "Deleted"
                    else:
                        print("The file does not exist")
                        success = "File does not exist/ You do not have permission to this"
                    conn.sendall(str.encode(success))
                
                elif(finalmessage == "commit" and data[1] == "write"):

                    #implement write shared logic here
                    success =""
                    folder_path = data[2].split("/")[0]
                    file_name = data[2].split("/")[1]
                    fileNameWithFolder = folder_path + "/"+ file_name
                    if not (folder_path == "root" or folder_path == "shared"):
                        path += folder_path+"/"

                    if(os.path.exists(path) == False):
                            os.makedirs(path)
                    
                    print("Path I am checking for",path)
                    print("File Name I checking for",file_name)

                    print("Checking",os.path.exists(path+"/"+file_name))
                    if(folder_path == "shared"):
                        print("Looking for the file",file_name)
                        if data[0] in sharedPermissionsForFile :
                            print("Found the user in the shared system")
                            sharedFiles = {}
                            sharedFiles = sharedPermissionsForFile[data[0]]
                            print("Shared files",sharedFiles)
                            print
                            if file_name in sharedFiles:
                                print("User has the file in his/her shared system")
                                actualFileName = sharedFiles[file_name][0]
                                actualUser = sharedFiles[file_name][1]
                                #path += actualUser
                                actualUser = System_encrypt.decrypt(actualUser).decode('utf-8')
                                grantedUser = data[0]
                                grantedUser = System_encrypt.decrypt(grantedUser).decode('utf-8')
                                print("Actual User",actualUser,grantedUser)

                               
                                if data[0]  in permissionsForFile[actualFileName]['write']:
                                     #decrypt the content to get the actual text
                                    E_decrypt = encryptdecrypt.Encrypt_Decrypt(grantedUser)

                                    contenttoWrite = ""
                                    for i in range(3,len(data)):
                                        contenttoWrite+=str(E_decrypt.decrypt(data[i]).decode('utf-8'))
                                        contenttoWrite += " "
                                
                                    #encrypt it with the original user's key

                                    E_encrypt = encryptdecrypt.Encrypt_Decrypt(actualUser)
                                    contenttoWrite = E_encrypt.encrypt(contenttoWrite).decode('utf-8')

                                    print("the path that I am going to write in",path)
                                    with open(path+"/"+actualFileName,'w') as fp:
                                        fp.write(contenttoWrite)
                                    
                                    fp.close()

                                    print("Here are the file contents",contenttoWrite)
                                    success = contenttoWrite
                                    #conn.sendall(str.encode(contenttoWrite))
                                else:
                                    success = "No Permission"
                                    #conn.sendall(str.encode(success))

                            else:
                                print("Did not find the file in the shared system of user")
                                success = "No Permission"
                                #conn.sendall(str.encode(success))
                        else:
                            print("Did not find the user in the shared system")
                            success = "No Permission"
                            #conn.sendall(str.encode(success))

                    elif os.path.exists(path+"/"+file_name) == False:
                        #grant all the permission to the user as we are creating the file now
                        print("Looks like the file does not exist in the path",path)
                        fileNameWithUser = data[0]+"/"+data[2]
                        permissionsForFile[fileNameWithUser] = {}
                        permissionsForFile[fileNameWithUser]['read'] = []
                        permissionsForFile[fileNameWithUser]['write'] = []
                        permissionsForFile[fileNameWithUser]['rename'] = []
                        permissionsForFile[fileNameWithUser]['read'].append(data[0])
                        permissionsForFile[fileNameWithUser]['write'].append(data[0])
                        permissionsForFile[fileNameWithUser]['rename'].append(data[0])

                        with open('permission_data.json', 'w') as outfile:
                            json.dump(permissionsForFile, outfile)
                    
                    else:
                        print("By any chance are we coming here")
                        if not data[0] in permissionsForFile[data[0]+"/"+data[2]]['write']:
                          success = "No Permission"

                    if(len(success) == 0):
                            f = open(path +  file_name, "w")
                            for i in range(3,len(data)):
                                f.write(data[i])
                                f.write(" ")
                            f.close()                        
                            success = "write operation was a success"
                    conn.sendall(str.encode(success))
                
                elif (finalmessage == "commit" and data[1] == "grant"):

                    print("In the if clause for the grant",data)
                    toUser = data[4]
                    fileNameToUser = data[3]
                    fileNameFromUser = data[2]
                    fromUser = data[0]
                    permissionsForFile[fromUser+"/"+fileNameFromUser][data[5]].append(toUser)

                    print("Grant",toUser,fileNameFromUser,fileNameToUser,data[2])
                    print("From User",fromUser)


                    permissions_lock.acquire()
                    try:

                        with open('permission_data.json', 'w') as outfile:
                            json.dump(permissionsForFile, outfile)
                    
                    finally:
                        permissions_lock.release()

                    

                    sharedPermissionsForFile[toUser] = {}

                    sharedPermissionsForFile[toUser][fileNameToUser] = []

                    sharedPermissionsForFile[toUser][fileNameToUser].append(fromUser+"/"+fileNameFromUser)
                    sharedPermissionsForFile[toUser][fileNameToUser].append(fromUser)

                    sharedPermission_lock.acquire()
                    try:

                        with open('sharedPermission_data.json', 'w') as outfile:
                            json.dump(sharedPermissionsForFile, outfile)
                    
                    finally:
                        sharedPermission_lock.release()
                    
                    conn.sendall(str.encode("Permission Granted"))


                elif(finalmessage == "commit" and data[1]=="list"):
                    userspath = "./"
                    userspath += data[0]
                    files_list = []
                    for currentpath, folders, files in os.walk(userspath):
                        for file in files:
                            print(os.path.join(currentpath, file))
                            files_list.append(os.path.join(currentpath, file))
                   
                    shared_list = []
                    if data[0] in sharedPermissionsForFile:
                        for files in sharedPermissionsForFile[data[0]] :
                            if len(sharedPermissionsForFile[data[0]][files]) >=1 :
                                shared_list.append(files+"-"+"shared")
                    
                    files_list.extend(shared_list)
                    print("Files list",files_list,len(files_list))
                    if(len(files_list) ==  0):
                           conn.sendall(str.encode("No Files"))         
                    
                    else:
                        files_list = ' '.join(files_list)
                        print("Sending the following files list",files_list) 
                        conn.sendall(str.encode(files_list)) 
                        
                else:
                    conn.sendall(str.encode("Aborted"))
    conn.close()


def main():

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((Host,Port))
    server.listen()
    print("Storage server is listening")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=Server_copy, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")

main()