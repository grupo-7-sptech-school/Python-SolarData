import psutil as p
from mysql.connector import connect, Error
import time

def insercao():
    """Teste rápido de conexão e inserção"""
    config = {
        'user': "solardata",
        'password': "Solar@Data01",
        'host': '34.198.76.254' , 
        'database': "solarData01",
        'port': 3306
    }

    try:
        db = connect(**config)
        if db.is_connected():
            print('Connected to MySQL server')
            
            with db.cursor() as cursor:
                cpu_percent = p.cpu_percent(interval=1)
                ram_percent = p.virtual_memory().percent
                disco_percent = p.disk_usage("/").percent
                
                print(f"Dados capturados:")
                print(f"  CPU: {cpu_percent:.1f}%")
                print(f"  RAM: {ram_percent:.1f}%") 
                print(f"  DISCO: {disco_percent:.1f}%")
                
                # Inserir cada componente
                fk_comp1 = "1"
                fk_comp2 = "2"
                fk_comp3 = "3"
                
                while True:
                    query = """INSERT INTO Registro(captura, fkComponente) 
                              VALUES (%s, %s)"""
                    cursor.execute(query, (cpu_percent, fk_comp1))
                    print(f"✓ Inserido componente {fk_comp1}")

                    db.commit()
                    print("Inserindo Registros")

                    query = """INSERT INTO Registro(captura, fkComponente) 
                                VALUES (%s, %s)"""
                    cursor.execute(query, (ram_percent, fk_comp2))
                    print(f"✓ Inserido componente {fk_comp2}")

                    db.commit()
                    print("Inserindo Registros")

                    query = """INSERT INTO Registro(captura, fkComponente) 
                                VALUES (%s, %s)"""
                    cursor.execute(query, (disco_percent, fk_comp3))
                    print(f"✓ Inserido componente {fk_comp3}")
                    
                    db.commit()
                    print("Inserindo Registros")
            
                    time.sleep(2)

            cursor.close()
            db.close()
    
    except Error as e:
        print('Error to connect with MySQL -', e)

    except KeyboardInterrupt:
        print(f"\nParando...")


insercao()