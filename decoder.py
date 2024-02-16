from datetime import datetime
import pickle
import redis

class Decoder:
    def __init__(self, payload, imei):
        self.payload = payload
        self.imei = imei
        self.precision = 10000000.0
        self.r_cli = redis.Redis(host='localhost', port=6379, db=0)

    def decode_data(self) -> int:
        # Four Zero Bytes
        # FourZeroBytes = payload[:8]

        # Header Bytes
        # dataLength = payload[8:16]

        # Codec ID
        # CodecID = payload[16:18]
        # CodecID = int(CodecID, 16)

        # number_of_rec
        number_of_rec = self.payload[18:20]
        number_of_rec = int(number_of_rec, 16)

        # number_of_rec_end
        number_of_rec_end = self.payload[len(self.payload)-10:-8]
        number_of_rec_end = int(number_of_rec_end, 16)

        avl_data = self.payload[20:-10]

        if number_of_rec == number_of_rec_end:
            _i = 0
            position = 0
            while _i < number_of_rec:

                # Time Stamp
                timestamp_hex = avl_data[position:position+16]
                timestamp_int = int(timestamp_hex, 16)
                timestamp = datetime.utcfromtimestamp(timestamp_int/1e3)
                position += 16

                # Priority
                priority_hex = avl_data[position:position+2]
                priority = int(priority_hex, 16)
                position += 2

                # Longitude
                longitude_hex = avl_data[position:position+8]
                longitude_int = int(longitude_hex, 16)
                longitude = longitude_int / self.precision
                position += 8

                # Latitude
                latitude_hex = avl_data[position:position+8]
                latitude_int = int(latitude_hex, 16)
                latitude = latitude_int / self.precision
                position += 8

                # Altitude
                altitude_hex = avl_data[position:position+4]
                altitude = int(altitude_hex, 16)
                position += 4

                # Angle
                angle_hex = avl_data[position:position+4]
                angle = int(angle_hex, 16)
                position += 4

                # Satellites
                satellites_hex = avl_data[position:position+2]
                satellites = int(satellites_hex, 16)
                position += 2

                # Speed
                speed_hex = avl_data[position:position+4]
                speed = int(speed_hex, 16)
                position += 4

                # SensorsData

                # IO element ID of Event generated
                io_event_code = int(avl_data[position:position + 2], 16)
                position += 2

                number_of_io_elements = int(avl_data[position:position + 2], 16)
                position += 2

                # 1 Bit
                number_of_io1_bit_elements = int(avl_data[position:position + 2], 16)
                position += 2
                io_data = dict()
                for _ in range(number_of_io1_bit_elements):
                    io_code = int(avl_data[position:position + 2], 16)
                    position += 2
                    io_val = int(avl_data[position:position + 2], 16)
                    position += 2
                    io_data[io_code] = io_val

                # 2 Bit
                number_of_io2_bit_elements = int(avl_data[position:position + 2], 16)
                position += 2

                for _ in range(number_of_io2_bit_elements):
                    io_code = int(avl_data[position:position + 2], 16)
                    position += 2
                    io_val = int(avl_data[position:position + 4], 16)
                    position += 4
                    io_data[io_code] = io_val

                # 4 Bit
                number_of_io4_bit_elements = int(avl_data[position:position + 2], 16)
                position += 2

                for _ in range(number_of_io4_bit_elements):
                    io_code = int(avl_data[position:position + 2], 16)
                    position += 2
                    io_val = int(avl_data[position:position + 8], 16)
                    position += 8
                    io_data[io_code] = io_val

                # 8 Bit
                number_of_io8_bit_elements = int(avl_data[position:position + 2], 16)
                position += 2

                for _ in range(number_of_io8_bit_elements):
                    io_code = int(avl_data[position:position + 2], 16)
                    position += 2
                    io_val = int(avl_data[position:position + 16], 16)
                    position += 16
                    io_data[io_code] = io_val

                _i += 1

                _record = {
                    "IMEI": self.imei.decode("utf-8"),
                    "DateTime": timestamp,
                    "Priority": priority,
                    "GPS Data": {
                        "Longitude": longitude,
                        "Latitude": latitude,
                        "Altitude": altitude,
                        "Angle": angle,
                        "Satellites": satellites,
                        "Speed": speed,
                    },
                    "I/O Event Code": io_event_code,
                    "Number of I/O Elements": number_of_io_elements,
                    "I/O Data": io_data
                }

                self.r_cli.rpush('GPSSensorsData', pickle.dumps(_record))
                print('[+] Data added to redis')
            # CRC
            # Crc = payload[len(payload)-8:]
        print(f"[+] Total Records: {number_of_rec}")
        return number_of_rec
