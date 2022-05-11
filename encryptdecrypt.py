import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Util.Padding import pad, unpad
 
BLOCK_SIZE = 16

class Encrypt_Decrypt:
    def __init__(self,password):
        self.password = password

    def encrypt(self,raw):
        #print("The key for",self.password)
        private_key = hashlib.sha256(self.password.encode("utf-8")).digest()
        #print(private_key)
        #print("Private ",private_key)
        raw = self.pad(raw)
        cipher = AES.new(private_key, AES.MODE_ECB)
        #codecs.encode(msg, 'hex').decode("utf-8")
        return base64.b16encode(cipher.encrypt(str.encode(raw)))
    
    
    def decrypt(self,enc):
        #rint("The key for",self.password)
        private_key = hashlib.sha256(self.password.encode("utf-8")).digest()
        #print("Private ",private_key)
        #print("Data here when {} is {}".format(self.password,enc))
        enc = base64.b16decode(enc)
        cipher = AES.new(private_key, AES.MODE_ECB)
        return self.unpad(cipher.decrypt(enc))

    def pad(self,raw):
        return raw + (BLOCK_SIZE - len(raw) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(raw) % BLOCK_SIZE)

    def unpad(self,raw):
        return unpad(raw, BLOCK_SIZE)
 
# E1 = Encrypt_Decrypt("password")
# #First let us encrypt secret message
# encrypted = E1.encrypt("admin1234")
# print("Encrypt",encrypted)

# file1 = open("myfile.txt", "w")  # write mode
# file1.write(str(encrypted))
# file1.close()

# file1 = open(encrypted, "w")
# print("Output of Readlines after writing")
# file1.write("He")
# file1.close()



# Let us decrypt using our original password
# decrypted = E1.decrypt(encrypted)
# print("Decryption",bytes.decode(decrypted))