import sqlite3

connection = sqlite3.connect("biodata.db")
cursor = connection.cursor()

def fun1(file_contents):
    return [ x for x in file_contents if len(x) > 0]

def fun2(file_contents):
    return [ x.split('\t') for x in file_contents if x[0] != '#' ]


filename = "TCGA.BLCA.mutect.0e239d8f-47b0-4e47-9716-e9ecc87605b9.DR-10.0.somatic.maf"
with open(filename, 'r') as f_read:
    file_contents =f_read.read().split('\n')

parseo = fun2(fun1(file_contents))

sql_statement = "CREATE TABLE IF NOT EXISTS MAF (%s)" % ", ".join(parseo[0][:])
cursor.execute(sql_statement)
connection.commit()

sql_aux =sql_statement = "INSERT INTO MAF (%s)" % ", ".join(parseo[0][:])
for x in parseo[1:]:
    cursor.execute(sql_aux + " VALUES(%s)" % ", ".join('\''+ y.replace("'", "''") + '\'' for y in x))

connection.commit()
connection.close()


# parseo =[ x.split('\t') for x in parseo]
#
# with open("fichero_parseo", 'w') as f_write:
#     f_write.write("\n".join(parseo))

