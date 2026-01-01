#this lab is about implementing a simple heab file managment system :
import os , struct
PAGE_SIZE = 4096 
# 1 - Create an empty heap file :
def create_heap_file(file_name : str) :
    with open(file_name,"wb") as f :
        pass

# 2 - read page from heap file :
def read_page(file_name : str , page_number : int) :
    file_size = os.path.getsize(file_name)
    last_page_number = file_size // PAGE_SIZE - 1

    if page_number > last_page_number :
        raise ValueError(f"Page {page_number} does not exist in the file.")
    
    with open(file_name , 'rb') as f :
        f.seek(page_number * PAGE_SIZE)
        page_data = f.read(PAGE_SIZE)
    return page_data

# 3 - append page data to the end of a heap file :
def append_page(file_name : str , page_data : bytes) :
    if len(page_data) != PAGE_SIZE :
        raise ValueError(f"Page data must be exactly {PAGE_SIZE}")
    
    with open(file_name , "ab") as f :
        f.write(page_data)

# 4 - write data to specific page in the heap file :
def write_page(file_name : str , page_number : int , data : bytes) :
    file_size = os.path.getsize(file_name)
    last_page_number = file_size // PAGE_SIZE - 1

    if page_number > last_page_number :
        raise ValueError(f"Page {page_number} does not exist in the file.")
    
    if len(data) != PAGE_SIZE :
        raise ValueError(f"Data must be exactly {PAGE_SIZE}")
    
    with open(file_name , 'r+b') as f :
        f.seek(page_number * PAGE_SIZE)
        f.write(data)

# 5 - clculate free space in a page :
def get_free_space_offset(page_data : bytes) :
    free_space_offset = int.from_bytes(page_data[4094 : 4096],"big")
    return free_space_offset

def get_slots_number(page_data : bytes) :
    number_of_records = int.from_bytes(page_data[4092:4094],"big")
    return number_of_records
def clculate_free_space(page_data : bytes) :
    free_space_offset = get_free_space_offset(page_data)
    number_of_records = get_slots_number(page_data)
    free_space = (PAGE_SIZE - free_space_offset) - ((number_of_records * 4 ) + 4)
    return free_space

def get_page_free_space(file_name : str ,page_number : int) :
    page_data = read_page(file_name,page_number)
    free_space = clculate_free_space(page_data)
    return free_space

# 6 - insert a record to a page : 
def insert_record_data_to_page(page_data : bytes, record_data : bytes) : 
    free_space = clculate_free_space(page_data)
    data_length = len(record_data)
    if(free_space < data_length) :
        raise ValueError("Not enough free space to insert the record.")
    else :
        free_space_offset = get_free_space_offset(page_data)
        slot_count = get_slots_number(page_data)
        page_data = (page_data[0:free_space_offset] + record_data + page_data[free_space_offset + len(record_data): 4096])
        #inserting the new slot entry :
        start_of_new_slot_entry = (PAGE_SIZE - (slot_count * 4)) - 8
        page_data = page_data[0: start_of_new_slot_entry] + free_space_offset.to_bytes(2,"big")+data_length.to_bytes(2,"big")+page_data[start_of_new_slot_entry + 4 : 4096]
        #updating slot count and free space offset :
        new_free_sapce_offset = free_space_offset + data_length 
        page_data = page_data[0: 4092] + (slot_count + 1).to_bytes(2,"big")+new_free_sapce_offset.to_bytes(2,"big")
        return page_data
    
# 7 - insert record to file :
def insert_record_to_file(file_name : str , record_data : bytes) :
    page_number = 0
    inserted = False
    while not inserted :
        try : 
            page_data = read_page(file_name , page_number)
        except ValueError as e :
            #data = create_page(record_data)
            empty_page = b'\x00' * PAGE_SIZE
            data = insert_record_data_to_page(empty_page,record_data)
            append_page(file_name,data)
            inserted = True
        else :
           page_free_space = clculate_free_space(page_data)
           if page_free_space < len(record_data) :
               page_number += 1
           else :
            data = insert_record_data_to_page(page_data , record_data)
            write_page(file_name,page_number,data)
            inserted = True

# 8 - get record from a page :
def get_record_from_page(page_data : bytes , record_id : int) :
    records_count = get_slots_number(page_data)
    if(records_count < record_id) :
        raise ValueError(f"Record {record_id} does not exist in the page.")
    else :
        #get the record slot offset :
        record_slot_offset = PAGE_SIZE - ((record_id * 4) + 4)
        #get the record offset :
        record_offset = int.from_bytes(page_data[record_slot_offset : record_slot_offset + 2],"big")
        record_length = int.from_bytes(page_data[record_slot_offset + 2 : record_slot_offset + 4],"big")
        #read the record data : 
        record_data = page_data[record_offset : record_offset + record_length]
        return record_data

# 9 - get record from a file :
def get_record_from_file(file_name : str , page_number : int , record_id : int) : 
    page_data = read_page(file_name,page_number)
    record_data = get_record_from_page(page_data , record_id)
    return record_data

# 10 - get all records from a page :
def get_all_records_from_page(page_data : bytes) :
    records = []
    #get slots count :
    slots_count = get_slots_number(page_data)
    for i in range(4092 - (slots_count * 4) , 4092 , 4) :
        record_offset = int.from_bytes(page_data[i : i + 2],"big")
        record_length = int.from_bytes(page_data[i + 2 : i + 4],"big")
        records.append(page_data[record_offset:record_offset + record_length])
    return records

# 11 - get all records from file :
def get_all_records_from_file(file_name : str) :
    records  = []
    page_number = 0 
    while(True) :
        try :
            page_data = read_page(file_name,page_number)
        except ValueError as e :
            return records
        else :
            page_records = get_all_records_from_page(page_data)
            records.append(page_records)
            page_number += 1

#create_heap_file("dept_heap_file")