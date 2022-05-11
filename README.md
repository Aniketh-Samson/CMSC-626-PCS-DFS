# CMSC-626-PCS-DFS

# Architecture of Distributed File System
1. This setup consists of 3 Storage Servers namely(ServerA, ServerB and ServerC)
2. It also consists of a Master Server which processes all requests.
3. It serves multiple instances of clients.

# Code changes to be made before running the code
1. In each storage server file(ServerA, ServerB and ServerC), replace the path "path = 'C:/Users/samso/OneDrive/Desktop/ALL_DOCS/UMBC/PCS/Trial3'" with the current file location.
2. Make note of the ports being used in each file (Eg: ServerA.py - Port = 54321) and make sure they match Line 13 in Master.py(Storageport_A = [54321, 64321, 1237]). (if the Storage server ports are changed, change them in master.py respectively)
3. Make sure that the port on the Client.py and the Master.py are the same.
4. If you are deploying the storage servers and the master on different systems, make sure that you change Line 9 in Master.py(StorageServer = '0.0.0.0') to whatever the IPv4 address of the system running the storage servers(ServerA, ServerB and ServerC).

# Deploying the code
1. To run the system, run the following commands on multiple command prompts from the respective file directories.
2. To run ServerA, open the command prompt in the file directory containing ServerA.py and execute 'python ServerA.py'
3. Similarly, run 'python ServerB.py' and 'python ServerC.py' in their respective command prompts.
4. To run the master, open the command prompt in the file directory containing Master.py and execute 'python Master.py'
5. Once these files are running, Client.py can be run by executing 'python Client.py'