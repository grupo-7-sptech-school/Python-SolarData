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
                
                fk_comp1 = "1"
                fk_comp2 = "2"
                fk_comp3 = "3"
                
                while True:

                    cpu_percent = p.cpu_percent(interval=1)
                    ram_percent = p.virtual_memory().percent
                    disco_percent = p.disk_usage("/").percent
                    
                    print(f"Dados capturados:")
                    print(f"  CPU: {cpu_percent:.1f}%")
                    print(f"  RAM: {ram_percent:.1f}%") 
                    print(f"  DISCO: {disco_percent:.1f}%")

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

                    io_before = p.disk_io_counters()
                    time.sleep(1)
                    io_after = p.disk_io_counters()

                    taxaLeitura = (io_after.read_bytes - io_before.read_bytes) / 1024 / 1024
                    taxaEscrita = (io_after.write_bytes - io_before.write_bytes) / 1024 / 1024

                    processos = []
                    for proc in p.process_iter(['name', 'memory_info']):
                        try:
                            uso = proc.info['memory_info'].rss / 1024 / 1024
                            processos.append((proc.info['name'], uso))
                        except:
                            pass

                    processos.sort(key=lambda x: x[1], reverse=True)
                    top1, top2, top3 = processos[:3]

                    query = """
                        INSERT INTO registroDisco
                        (fkMaquina, taxaLeitura, taxaEscrita,
                        top1, top1Valor,
                        top2, top2Valor,
                        top3, top3Valor)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """

                    cursor.execute(query, (
                        "1",
                        taxaLeitura, taxaEscrita,
                        top1[0], top1[1],
                        top2[0], top2[1],
                        top3[0], top3[1]
                    ))

                    db.commit()
                    print("registroDisco inserido")


            cursor.close()
            db.close()
    
    except Error as e:
        print('Error to connect with MySQL -', e)

    except KeyboardInterrupt:
        print(f"\nParando...")


insercao()
