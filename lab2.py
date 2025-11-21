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
    schema = load_schema("schema.json")
    table = get_table(table_name,schema)
    
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
    schema = load_schema("schema.json")


#def insert_structured_record(table_name, schema, record_dict):



#def read_all_structured_records(table_name, schema):