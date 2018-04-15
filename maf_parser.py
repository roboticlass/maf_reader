import sqlite3
import gzip
import os

""" Mutation Annotation Format Parser """


#Firstly we define every function
def fun0(filename):
    with gzip.open(filename, 'rt') as f_read:
        return [line.split('\t') for line in f_read if not line.startswith('#')]


def fun4(cursor, parsed):
    headers= "Project_ID, " + ", ".join(parsed[0][:])
    columns = "MAF (%s)" % headers
    sql_statement = "CREATE TABLE IF NOT EXISTS " + columns
    cursor.execute(sql_statement)
    return columns

def fun5(cursor, parsed, table_columns, project_id):
    """ First Version: Based on executemany """
    sql_aux = "INSERT INTO " + table_columns+ " VALUES(" + "?," * 120 + "?)"
    parsed_t = tuple(map(tuple,[ [project_id]+x for x in parsed[1:]]))
    cursor.executemany(sql_aux , parsed_t)

def connect_db(db_name):
    connection =sqlite3.connect(db_name)
    cursor=connection.cursor()
    return connection,cursor

def create_db(db_name):
    connection =sqlite3.connect(db_name)
    cursor=connection.cursor()

    cursor.execute('PRAGMA synchronous = OFF')
    cursor.execute('PRAGMA journal_mode = MEMORY')
    cursor.execute('PRAGMA locking_mode = Exclusive')
    cursor.execute('PRAGMA page_size = 4096')
    connection.commit()

    return connection,cursor


#Secondly, we define the main function that  will carry out all the functions before defined.
def main_func(filename, project_id):

    # Objects connected to the database
    # connection allows to do .commit() .close()
    # cursor allows .execute("sql") .executemany("sql") .fetchone() .fetchall()
    connection, cursor = connect_db('biodata.db')

    # Function 0: Filename reader
    file_contents = fun0(filename)


    # Function 4: creation of MAF table in the database
        #   - Collect the column names to parsed[0]
    table_columns = fun4(cursor, file_contents)

    # Commit to confirm the CREATE TABLE from fun4
    connection.commit()

    # Function 5: Insert data into MAF table
            # INSERT sintax: INSERT INTO MAF ('COL1,'COL2',...,'COL120') VALUES ('VAL1',...,'VAL120')
            # table_columns from fun4 to take advantage of the merged "MAF ('COL1,'COL2',...,'COL120')"
    fun5(cursor, file_contents, table_columns,project_id)

    # Commit from the insert and close the database
    connection.commit()
    connection.close()
    #At this point, it is finished the parsed part of main function.
    # Parsed finished

def find_files():
    path = []
    filenames = []
    for root, dirs, files in os.walk('.'):
        for file_ in files:
            if '.gz' in file_:
                path.append(os.path.join(root, file_))
                filenames.append(file_[0:file_.index('.mutect.')])
    return path, filenames

def delete_db():
    # conn = sqlite3.connect("biodata.db")
    # cursor = conn.cursor()
    # cursor.execute('DROP TABLE IF EXISTS MAF')
    # conn.commit();
    # conn.close()
    try:
        os.remove('biodata.db')
    except OSError:
        pass


def parser_single():
    print("Deleting old DB ")

    """ WE START DELETING MAF TABLE IF IT EXISTS"""
    delete_db()
    create_db('biodata.db')

    print('Starting parsing process: GETTING PATHs ')

    """ First Version: We find out all the .maf.gz files within the working directory"""
    path, filenames = find_files()

    print('PATHs READY\n\nStarting processing of files: ')


    for x, y in zip(path, filenames):
        print("Parsing file: " + x)
        main_func(x, y)
        print("File parsed")

    print('ALL FILES PARSED\n')

def parser_multi():
    from concurrent.futures import ProcessPoolExecutor,ALL_COMPLETED,wait

    workers=4
    futures=[]

    print("Deleting old DB ")

    """ WE START DELETING MAF TABLE IF IT EXISTS"""
    delete_db()

    print('Starting parsing process: GETTING PATHs ')

    """ First Version: We find out all the .maf.gz files within the working directory"""
    path, filenames = find_files()
    print(filenames)

    print('PATHs READY\n\nStarting processing of files: ')

    with ProcessPoolExecutor(workers) as ppool:
        for x, y in zip(path, filenames):
            futures.append(ppool.submit(main_func,x, y))
        wait(futures,return_when=ALL_COMPLETED)

    print('ALL FILES PARSED\n')

if __name__=='__main__':
    parser_multi()




