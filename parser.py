import sqlite3

""" Mutation Annotation Format Parser """


def fun0(filename):
    with open(filename, 'r') as f_read:
        return f_read.read().split('\n')


def fun1(file_contents):
    return [ x for x in file_contents if len(x) > 0]


def fun2(file_contents):
    return [ x.split('\t') for x in file_contents if x[0] != '#' ]


def fun3(file_contents):
    return [ x.split('\t') for x in file_contents if len(x) > 0 and not x.startswith('#')]


def fun4(cursor, parsed):
    columns = "MAF (%s)" % ", ".join(parsed[0][:])
    sql_statement = "CREATE TABLE IF NOT EXISTS " + columns
    cursor.execute(sql_statement)
    return columns

def fun5(cursor, parsed, table_columns):
    """ First Version: Based on executemany """
    sql_aux = "INSERT INTO " + table_columns
    parsed_t = tuple(map(tuple, parsed[1:]))
    cursor.executemany(sql_aux + " VALUES(" + "?," * 119 + "?)", parsed_t)


def main_func():

    """ AMPLIACIÓN 1"""
    # Nombre del fichero a parsear
    # Bucle que busque los distintos ficheros a parsear, crear una lista que contenga las direcciones
    # Tipo PATH/TCGA.BLCA.mutect.0e239d8f-47b0-4e47-9716-e9ecc87605b9.DR-10.0.somatic.maf
    filename = "TCGA.BLCA.mutect.0e239d8f-47b0-4e47-9716-e9ecc87605b9.DR-10.0.somatic.maf"

    # Comenzando parseo
    print("Comenzando parseo fichero: " + filename)

    # Objetos de conexión con la base de datos
    # connection permite hacer .commit() .close()
    # cursor permite .execute("sql") .executemany("sql") .fetchone() .fetchall()
    connection = sqlite3.connect("biodata.db")
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS MAF')

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
    print("Terminado parseo fichero: " + filename)

""" AMPLIACION 2: AÑADIR SOPORTE PARA MYSQL O BASE DE DATOS REMOTA """
main_func(nombre)


