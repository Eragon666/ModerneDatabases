from encode import encode, decode
from checksum import add_integrity, check_integrity
from struct import Struct
from os import SEEK_SET, SEEK_CUR, SEEK_END

class fileIO(object):
    def __init__(self, filename):
        self.filename = filename
        self.struct = Struct('<BLL')
        self.parsed = False

        try:
            f = open(filename, "br")
        except FileNotFoundError:
            print("Opgevraagd bestand niet gevonden. Een nieuw bestand is aangemaakt!")
            f = open(filename, "w")
            f.close()
            return None

        f.close()

        pass

    def parse_header(self):

        f = open(self.filename, "br")
        self.offset = f.tell();

        data = f.read(self.struct.size)

        if len(data) < self.struct.size:
            raise EOFError('no more chunks available.')

        self._id, self.size, self.checksum = self.struct.unpack(data)
        self.parsed = True

    def read(self):

        if not self.parsed:
            self.parse_header()

        f = open(self.filename, "br")

        i = 0
        read_till = 0

        while True:
            try:
                f.seek(-i,SEEK_CUR)
            except OSError:
                print("Footer niet gevonden, bestand is incorrect. Error!")
                f.close()
                return None

            data = f.read(i-read_till)

            try:
                footer = decode(data)
                #print(footer)

                if b"root_offset" in footer:
                    break
                else:
                    read_till = i

            except:
                i += 1

        print("Footer: " + str(footer))

        return footer

    def write(self, data):

        f = open(self.filename, "ba")
        offset = f.tell()
        f.write(add_integrity(encode(data)))
        f.close()
        return offset