import csv


# create list of rows
def create_row_list(file_path_list, target_row_list):
    for f in file_path_list:
        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile)
            headers = next(csvreader) # skip header
            if headers != None: 
                for line in csvreader:
                    target_row_list.append(line)
    return target_row_list

# write output data
def write_output_csv(target_file, headers, rows_list):
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open(target_file, 'w+', encoding='utf-8', newline='') as f:

        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(headers)
        for row in rows_list:
            if (row[0] ==''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
    
    return