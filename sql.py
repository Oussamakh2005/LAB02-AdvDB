#SQL PROCESSOR
import json
import records_managment

def load_schema(schema : str) -> dict :
    with open(schema,"r") as f :
        return json.load(f)
def process_values(s : str):
    s = s.strip()
    if(s.startswith("'")) :
        s = s[1 : -1]
    try:
        num = float(s)
        return num
    except ValueError:
        return s
def filter_records(records : list , parsed_query ) -> dict :
    filtred_records = []
    for record in records :
        if parsed_query["condition"] == None or record[parsed_query["condition"]["field"]] == parsed_query["condition"]["value"] :
            new_record = {}
            if(parsed_query["fields"][0] == "*") :
                new_record = record
            else :
              for field in parsed_query["fields"] :
                new_record[field] = record[field]
            filtred_records.append(new_record)
    return filtred_records
def parse_select_query(query : str, schema) -> dict:
    #schema_dic = load_schema(schema)
    # 1 - remove the semicolumn
    query = query[0 : -1]
    # 2 - get the table name 
    splited_query = query.split(" ")
    table = splited_query[splited_query.index("FROM") + 1]
    # 2 - get the condition
    condition = None
    if "WHERE" in query : 
       condition = query.split("WHERE")[1].split("=")
       condition = {"field":condition[0].strip() , "value":process_values(condition[1])}
    # 3 - get the fields 
    fields = query.split("SELECT")[1].split('FROM')[0].split(',')
    fields = fields = [field.strip() for field in fields]
    # 4 - return the structured dictionary
    return {
        "fields" : fields,
        "table" : table,
        "condition" : condition
    }

def parse_insert_query(query : str, schema) -> dict:
    # 1 - remove the semicolumn
    query = query[0 : -1]
    # 2 - get table name  
    splited_query = query.split(" ")
    table = splited_query[splited_query.index("INTO") + 1]
    # 3 - get fields 
    fields = query.split(table)[1].split("VALUES")[0].strip()[1:-1].split(',')
    fields = fields = [field.strip() for field in fields]
    # 4 - get values 
    values = query.split("VALUES")[1].strip()[1:-1].split(",")
    #values = [value.strip() for value in values]
    #values = [value[1:-1] if value.startswith("'") else value.strip() for value in values]
    values = [process_values(value) for value in values]
    # 4 - return the structured dictionary
    return {
        "table" : table,
        "fields" : fields,
        "values" : values
    }
def execute_select_query(query : str , schema : str) :
    stracture = parse_select_query(query, "schema.json")
    records = records_managment.read_all_structured_records(stracture["table"],schema)
    filterd_records = filter_records(records,stracture)
    return filterd_records

def execute_insert_query(query : str , schema : str) :
    stracture = parse_insert_query(query, "schema.json")
    record = {}
    for i in range(0 , len(stracture["fields"])) :
        record[stracture["fields"][i]] = stracture["values"][i]
    records_managment.insert_structured_record(stracture["table"] , schema , record)
    return True
def execute_query(query, schema):
    if query.startswith("SELECT") :
        return execute_select_query(query,schema)
    elif query.startswith("INSERT") :
        execute_insert_query(query,schema)
    else :
        raise ValueError("Invalid query")

"""query = "SELECT name, salary FROM Employee WHERE id = 3;"
schema = parse_select_query(query, "schema.json")
print(schema)
query = "INSERT INTO Employee (id, name, salary) VALUES (4, 'Alice', 4500);"
schema = parse_insert_query(query,"schema.json")
print(schema)"""

"""query = "INSERT INTO Employee (id, name, salary) VALUES (4, 'Alice', 4500);"
execute_query(query,"schema.json")
#query = "INSERT INTO Employee (id, name, salary) VALUES (5, 'Oussama', 14500);"
#execute_query(query,"schema.json")
query = "SELECT name , salary FROM Employee WHERE id = 5;"
records = execute_query(query,"schema.json")
for record in records :
    print(record)"""
