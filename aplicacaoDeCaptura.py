import psutil as p
from mysql.connector import connect, Error
import time
import random

def insercao():
    """Teste rápido de conexão e inserção"""
    config = {
        'user': "solardata",
        'password': "Solar@Data01",
        'host': '34.198.76.254',
        'database': "solarData01",
        'port': 3306
    }

    try:
        db = connect(**config)
        if db.is_connected():
            print('Connected to MySQL server')
            
            with db.cursor() as cursor:

                while True:
                    cpu_percent = p.cpu_percent(interval=1)
                    ram_percent = p.virtual_memory().percent
                    disco_percent = p.disk_usage("/").percent

                    
                    print(f"Dados capturados:")
                    print(f"  CPU: {cpu_percent:.1f}%")
                    print(f"  RAM: {ram_percent:.1f}%") 
                    print(f"  DISCO: {disco_percent:.1f}%")

                    fk_comp1 = 1
                    fk_comp2 = 2
                    fk_comp3 = 3
                

                    ##---------- Dados de Consumo ----------##

                    if cpu_percent > 90:
                        potencia = random.uniform(750,1000)
                    elif cpu_percent >= 20:
                        potencia = random.uniform(150,700)
                    else: 
                        potencia = random.uniform(30,150)

                    queryConsumo = """
                                INSERT INTO ConsumoEnergia (data, fkMaquina, potencia, intervalo_medicao)
                                VALUES (NOW(), %s, %s, %s)
                            """
                    
                    cursor.execute(queryConsumo, (1598329989, potencia, 1))
                    db.commit()
                    
                    print(f"Consumo Energia inserindo: {potencia:.2f} W")


                    ##-------------- Coleta de Disco ---------------##

            #         io_before_total = p.disk_io_counters(perdisk=False)
            #         time.sleep(1)
            #         io_after_total = p.disk_io_counters(perdisk=False)

            #         taxaLeitura = (io_after_total.read_bytes - io_before_total.read_bytes) / 1024 / 1024
            #         taxaEscrita = (io_after_total.write_bytes - io_before_total.write_bytes) / 1024 / 1024

            #         processos = []
            #         for proc in p.process_iter(['name', 'memory_info']):
            #             try:
            #                 uso = proc.info['memory_info'].rss / 1024 / 1024
            #                 processos.append((proc.info['name'], uso))
            #             except:
            #                 pass

            #         processos.sort(key=lambda x: x[1], reverse=True)
            #         top1, top2, top3 = processos[:3]

            #         processosLeitura = []
            #         processosEscrita = []

            #         for proc in p.process_iter(['name', 'io_counters']):
            #             try:
            #                 io = proc.info['io_counters']
            #                 if io:
            #                     processosLeitura.append((proc.info['name'], io.read_bytes))
            #                     processosEscrita.append((proc.info['name'], io.write_bytes))
            #             except:
            #                 pass

            #         processosLeitura.sort(key=lambda x: x[1], reverse=True)
            #         processosEscrita.sort(key=lambda x: x[1], reverse=True)

            #         procMaisLeitura, procMaisLeituraValor = processosLeitura[0]
            #         procMaisEscrita, procMaisEscritaValor = processosEscrita[0]

            #         queryRegistro = """INSERT INTO Registro(captura, fkComponente) VALUES (%s, %s)"""
            #         cursor.execute(queryRegistro, (cpu_percent, fk_comp1))
            #         db.commit()

            #         cursor.execute(queryRegistro, (ram_percent, fk_comp2))
            #         db.commit()

            #         cursor.execute(queryRegistro, (disco_percent, fk_comp3))
            #         db.commit()

            #         query = """
            #             INSERT INTO registroDisco
            #             (fkMaquina, taxaLeitura, taxaEscrita,
            #             top1, top1Valor,
            #             top2, top2Valor,
            #             top3, top3Valor,
            #             procMaisLeitura, procMaisLeituraValor,
            #             procMaisEscrita, procMaisEscritaValor)
            #             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            #         """


            #         cursor.execute(query, (
            #             "1",
            #             taxaLeitura, taxaEscrita,
            #             top1[0], top1[1],
            #             top2[0], top2[1],
            #             top3[0], top3[1],
            #             procMaisLeitura, procMaisLeituraValor,
            #             procMaisEscrita, procMaisEscritaValor
            #         ))
            #         db.commit()

            #         print("registroDisco inserido")

            # cursor.close()
            # db.close()

    except Error as e:
        print('Error to connect with MySQL -', e)

    except KeyboardInterrupt:
        print(f"\nParando...")

insercao()
