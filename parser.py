import sqlite3

""" Mutation Annotation Format Parser """


def fun0(filename):
    with gzip.open(filename, 'rt') as f_read:
        return [line.split('\t') for line in f_read if len(line)>0 and not line.startswith('#')]


def fun3(file_contents):
    return [ x.split('\t') for x in file_contents if len(x) > 0 and not x.startswith('#')]


def fun4(cursor, parsed):
    columns = "MAF (%s)" % ", ".join(parsed[0][:])
    sql_statement = "CREATE TABLE IF NOT EXISTS " + columns
    cursor.execute(sql_statement)
    return columns

def fun5(cursor, parsed, table_columns):
    """ First Version: Based on executemany """
    sql_aux = "INSERT INTO " + table_columns+ " VALUES(" + "?," * 119 + "?)"
    parsed_t = tuple(map(tuple, parsed[1:]))
    cursor.executemany(sql_aux , parsed_t)


def main_func(filename):

    # Comenzando parseo
    print("Parsing file: " + filename)

    # Objetos de conexión con la base de datos
    # connection permite hacer .commit() .close()
    # cursor permite .execute("sql") .executemany("sql") .fetchone() .fetchall()
    connection = sqlite3.connect("biodata.db")
    cursor = connection.cursor()

    # Función 0: Lectura del fichero filename
    file_contents = fun0(filename)

    # Función 3: Elimina los comentarios y las lineas '' y separa la lista por '\t'
        #   - Los comentarios son lineas que empiezan por '#'
        #   - Las lineas vacias son por los '\n' del final
    parsed = fun3(file_contents)

    # Funcion 4: creación de la tabla MAF en la base de datos
        #   - Los nombres de las columnas las cogemos de parsed[0]
    table_columns = fun4(cursor, parsed)

    # Commit para confirmar el CREATE TABLE de fun4
    connection.commit()

    # Función 5: Insert de los datos en la tabla MAF
            # Sintaxis INSERT: INSERT INTO MAF ('COL1,'COL2',...,'COL120') VALUES ('VAL1',...,'VAL120')
            # table_columns viene de fun4 para aprovechar que ya hemos concatenado "MAF ('COL1,'COL2',...,'COL120')"
    fun5(cursor, parsed, table_columns)

    # Commit de la insert y cerramos la base de datos
    connection.commit()
    connection.close()

    # Parseo terminado
    print("Finishing file: " + filename + '\n\n')


if __name__=='__main__':
    import os

    """ WE START DELETING MAF TABLE IF EXISTS"""
    conn = sqlite3.connect("biodata.db")
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS MAF')
    conn.commit(); conn.close()

    print('Starting parsing process: GETTING PATHs ')

    """ First Version: We find out all the .maf.gz files within the working directory"""
    path = []
    for dirname, dirnames, filenames in os.walk('.'):
        # print path to all subdirectories first.
        for subdirname in dirnames:
            for subsubdirname, subsubdirnames, sfilenames in os.walk(subdirname):
                for filename in sfilenames:
                    if '.gz' in filename:
                        path.append(".\\" + subdirname + "\\" + filename)

    print('PATHs READY\n\nStarting processing of files: ')

    for x in path:
        main_func(x)

    print('ALL FILES PARSED\n')


