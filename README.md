# maf_reader
Mutation Annotation Format Python Reader for GDC files (https://portal.gdc.cancer.gov/)

Use:

  - Download a single or multiple files from GDC Repository
  - Unzip the .tar and place the script into the folder.
  - The script should find out all the .maf.gz files within the directory, read them all and create a SQLITE3 database called "biodata.db".
  - You can now connect to this database through python or other tools like SQLiteStudio
