import sqlite3
import gzip

""" Mutation Annotation Format Parser """


#Firstly we define every function
def fun0(filename):
    with gzip.open(filename, 'rt') as f_read:
        return [line.split('\t') for line in f_read if len(line)>0 and not line.startswith('#')]


def fun3(file_contents):
    return [ x.split('\t') for x in file_contents if len(x) > 0 and not x.startswith('#')]


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

#Secondly, we define the main function that  will carry out all the functions before defined.
def main_func(filename, project_id):

    # Parsed initiate
    print("Parsing file: " + filename)

    # Objects connected to the database
    # connection allows to do .commit() .close()
    # cursor allows .execute("sql") .executemany("sql") .fetchone() .fetchall()
    connection = sqlite3.connect("biodata.db")
    cursor = connection.cursor()

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
    print("Finishing file: " + filename + '\n\n')


if __name__=='__main__':
    import os

    def delete_db():
        # conn = sqlite3.connect("biodata.db")
        # cursor = conn.cursor()
        # cursor.execute('DROP TABLE IF EXISTS MAF')
        # conn.commit();
        # conn.close()
        os.remove('biodata.db')

    def main():
        path=[]
        filenames=[]
        for root, dirs, files in os.walk('.'):
            for file_ in files:
                if '.gz' in file_:
                    path.append(os.path.join(root, file_))
                    filenames.append(file_)
        return path, filenames


    print("Deleting old DB ")


    """ WE START DELETING MAF TABLE IF IT EXISTS"""
    delete_db()

    print('Starting parsing process: GETTING PATHs ')

    """ First Version: We find out all the .maf.gz files within the working directory"""
    path, filenames = main()

    print('PATHs READY\n\nStarting processing of files: ')

    print(filenames[1][:])
    print(filenames[0][0: filenames[0].index('.mutect.')])

    for x,y in zip(path,filenames):
        main_func(x,y[0:y.index('.mutect.')])

    print('ALL FILES PARSED\n')

