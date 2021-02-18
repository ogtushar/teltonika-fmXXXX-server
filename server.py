import socket
from threading import Thread
from time import strftime, gmtime
import struct
import binascii
from decoder import Decoder

# Global Variables
host = '0.0.0.0'
port = 5555


class ClientThread(Thread):
    def __init__(self, _socket) -> None:
        Thread.__init__(self)
        self.conn = _socket[0]
        self.addr = _socket[1]
        self.identifier = "None"
        self.logTime = strftime('%d %b %H:%M:%S', gmtime())
        self.step = 1
        self.imei = "unknown"

    def log(self, msg):
        print(f"{self.logTime}\t{self.identifier}\t{msg}")
        pass

    def run(self):
        client = self.conn
        if client:
            self.identifier = self.addr[0]
            for _ in range(2):
                try:
                    buff = self.conn.recv(8192)
                    received = binascii.hexlify(buff)
                    if len(received) > 2:
                        if self.step == 1:
                            self.step = 2
                            self.imei = received
                            self.log("Device Authenticated | IMEI: {}".format(self.imei))
                            self.conn.send('\x01'.encode('utf-8'))
                        elif self.step == 2:
                            decoder = Decoder(payload=received, imei=self.imei)
                            len_records = decoder.decode_data()
                            if len_records == 0:
                                self.conn.send('\x00'.encode('utf-8'))
                                self.conn.close()
                            else:
                                self.conn.send(struct.pack("!L", len_records))
                                self.log("Done! Closing Connection")
                                self.conn.close()
                    else:
                        self.conn.send('\x00'.encode('utf-8'))
                except socket.error as err:
                    print(f"[+] Socket Error: {err}")
        else:
            self.log('Socket is null')


if __name__ == "__main__":
    print(f"VTS SERVER. {strftime('%d %b %H:%M:%S', gmtime())}")
    print(f"Server Started at port: {port}")
    server = None
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except socket.error as e:
        print("[+] Socket Creation Error: {}".format(e))
        exit(0)

    try:
        server.bind((host, port))
    except socket.error as e:
        print("[+] Socket Binding Error: {}".format(e))
        exit(0)

    try:
        server.listen(5)
    except socket.error as e:
        print("[+] Socket Listening Error: {}".format(e))
        exit(0)
    while True:
        ClientThread(server.accept()).start()

