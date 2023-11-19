import psycopg2

# Isi dengan informasi koneksi PostgreSQLmu
dbname = "dataRekdat"
user = "etl"
password = "endtoend"
host = "localhost"
port = "5432"

try:
    # Coba melakukan koneksi
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Koneksi berhasil!")
except Exception as e:
    print("Error koneksi:", str(e))
finally:
    # Tutup koneksi
    if conn:
        conn.close()
