import time
import os
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
flag=0

import os

def check_for_malicious():
    arr_txt = []
    filearr = []
    for root, dirs, files in os.walk("C:/Users/nsrag/OneDrive/Desktop/UMBC/SEM2/PCS/Project/Trial1/Multithreaded_Sever/ServerA"):
        for file in files:
            if (file.endswith(".txt")):
                arr_txt.append(file)
    with open("C:/Users/nsrag/OneDrive/Desktop/UMBC/SEM2/PCS/Project/Trial1/Multithreaded_Sever/filelist.txt", 'r') as file:
        lines = file.readlines()
        for line in lines:
            filearr.append(line.strip("\n"))
    print(arr_txt,filearr)
    if arr_txt == filearr:
        flag=0
        return flag
    else:
        flag=1
        return flag
check_for_malicious()
if __name__ == "__main__":
    patterns = ["*"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

def on_created(event):
    time.sleep(5)
    if check_for_malicious()==1:
        print(f"Malicious creation is performed on, {event.src_path} ")


def on_deleted(event):
    time.sleep(5)
    if check_for_malicious() == 1:
        print(f"Malicious detected is performed on, {event.src_path} ")

def  on_modified(event):
    time.sleep(5)
    print("in rename function")
    if check_for_malicious()==1:
        print(f"Malicious rename is performed on, {event.src_path} ")


my_event_handler.on_created = on_created
my_event_handler.on_deleted = on_deleted
#my_event_handler.on_modified = on_modified
path = "C:/Users/nsrag/OneDrive/Desktop/UMBC/SEM2/PCS/Project/Trial1/Multithreaded_Sever"
go_recursively = True
my_observer = Observer()
my_observer.schedule(my_event_handler, path, recursive=go_recursively)

my_observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    my_observer.stop()
    my_observer.join()