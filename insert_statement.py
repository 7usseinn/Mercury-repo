import pandas as pd
import lookups
 
def insert_statement(dataframe):
 columns = ','.join(dataframe.columns)
 for index, row in dataframe.iterrows():
    values_list = []
    for val in row.values:
        val_type = str(type(val))
        if val_type == lookups.HandledType.TIMESTAMP.value:
            values_list.append(str(val))
        elif val_type == lookups.HandledType.STRING.value:
            values_list.append(f"'{val}'")
        elif val_type == lookups.HandledType.LIST.value:
            val_item = ';'.join(val)
            values_list.append(f"'{val_item}'")
        else:
            values_list.append(str(val))
 
    values = ', '.join(values_list)
    insert_statement = f"INSERT INTO test_schema.test_table ({columns}) VALUES ({values});"
    print(insert_statement)