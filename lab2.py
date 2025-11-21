import heap_file
import json
import struct


def load_schema(schema : str) : 
    with open(schema,"r") as f :
        return json.load(f)
    
def get_table(table_name : str , schema) :
    for t in schema :
        if t["table_name"] == table_name :
            return t
    raise ValueError(f"Table {table_name} not found in schema.")
def encode_record(record_dict, table_name, schema) -> bytes:
    # check if the table name is valid
    schema_dic = load_schema(schema)
    table = get_table(table_name,schema_dic)
    # start encoding the record 
    record = b""
    for field in table["fields"] :
        name = field["name"]
        typ  = field["type"]
        value = record_dict[name]
        if(typ == "int") :
            record += struct.pack("!i",value)
        elif(typ == "float") :
            record += struct.pack("!f",value)
        elif(typ.startswith("char(")) :
            size = int(typ[5:-1])
            txt = str(value).encode("utf-8")
            if(len(txt) > size) :
                txt = txt[:size]
            else :
                txt += b" "*(size - len(txt))
            record += txt
        elif(typ.startswith("varchar(")) :
            size = int(typ[8:-1])
            txt = str(value).encode("utf-8")
            if(len(txt) > size) :
                txt = txt[:size]    
            record += struct.pack('!B',len(txt))
            record += txt
    return record

def decode_record(record_bytes, table_name, schema) -> dict:
    schema_dic = load_schema(schema)
    record = {}
    table = get_table(table_name,schema_dic)
    start_byte = 0
    for field in table["fields"] :
        name = field["name"]
        typ = field["type"]
        value = None
        if(typ == "int") :
            value = struct.unpack("!i",record_bytes[start_byte:start_byte + 4])[0]
            start_byte +=4
        elif(typ == "float") :
            value = struct.unpack("!f",record_bytes[start_byte:start_byte + 4])[0]
            start_byte +=4
        elif(typ.startswith("char(")) :
            size = int(typ[5 : -1])
            value = record_bytes[start_byte : start_byte + size].decode("utf-8").rstrip()
            start_byte += size
        elif(typ.starswith("varchar(")) :
            length = struct.unpack('!B',record_bytes[start_byte:start_byte + 1])
            start_byte += 1
            value = record_bytes[start_byte : start_byte + length].decode("utf-8")
            start_byte += length
        record[field["name"]] = value
    return record






#def insert_structured_record(table_name, schema, record_dict):



#def read_all_structured_records(table_name, schema):


#================= Test =====================
record = {
    "id" : 1,
    "name" : "John Doe",
    "salary" : 150.34
}
record_bytes = encode_record(record, "Employee", "schema.json")
print(f"Encoded record bytes: {record_bytes}")
decoded_record = decode_record(record_bytes, "Employee", "schema.json")
print(f"Decoded record: {decoded_record}")