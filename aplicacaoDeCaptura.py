import psutil as p
from mysql.connector import connect, Error
import time
# import socket
import random
# import hashlib

# hostname = socket.gethostname()
# hostnameHash = int(hashlib.sha256(hostname.encode()).hexdigest(), 16) % (10**10)
fk_maquina = 1598329989

def obter_ultimas_fk_componentes(config, fk_maquina):
    try:
        db = connect(**config)
        if db.is_connected():
            with db.cursor() as cursor:
                
                query = """
                    SELECT idComponente 
                    FROM Componente 
                    WHERE fkMaquina = %s
                    ORDER BY idComponente ASC
                    LIMIT 3
                """

                cursor.execute(query, (fk_maquina,))
                resultados = cursor.fetchall()

                if len(resultados) < 3:
                    print("Sua máquina não tem os componentes cadastrados.")
                    return None, None, None

                fk_comp1 = resultados[0][0]
                fk_comp2 = resultados[1][0]
                fk_comp3 = resultados[2][0]

                return fk_comp1, fk_comp2, fk_comp3

    except Error as e:
        print('Erro ao buscar fkComponente -', e)
        return None, None, None
    finally:
        if 'db' in locals() and db.is_connected():
            db.close()


def insercao():
    config = {
        'user': "root",
        'password': "Pipoca12200#",
        'host': "localhost",
        'database': "solarData01",
        'port': 3306
    }


    # def obter_fk_maquina(config):

    #     try:
    #         db = connect(**config)
    #         with db.cursor() as cursor:
    #             cursor.execute("SELECT hostName FROM maquina WHERE hostName = %s", (fk_maquina))
    #             result = cursor.fetchone()


    #             if result:
    #                  return fk_maquina

    #             else:
    #                 print(f"Máquina com host: '{fk_maquina}' não cadastrada!")
    #             return None

    #     except Error as e:
    #         print("Erro ao buscar fkMaquina:", e)
    #         return None
    #     finally:
    #         if 'db' in locals() and db.is_connected():
    #             db.close()


    try:
        db = connect(**config)
        if db.is_connected():
            print('Connected to MySQL server')
            
            # fk_maquina = obter_fk_maquina(config)
            
            fk_comp1, fk_comp2, fk_comp3 = obter_ultimas_fk_componentes(config, fk_maquina)

            
            print(f"FK Componentes atribuídos: {fk_comp1}, {fk_comp2}, {fk_comp3}")

            if not fk_comp1 or not fk_comp2 or not fk_comp3:
                print("Essa máquina não tem componentes suficientes cadastrados.")
                return

            
            # print("inserindo no hostname:" + socket.gethostname())
            with db.cursor() as cursor:

                while True:
                    cpu_percent = p.cpu_percent(interval=1)
                    ram_percent = p.virtual_memory().percent
                    disco_percent = p.disk_usage("/").percent


                    
                    print(f"Dados capturados:")
                    print(f"  CPU: {cpu_percent:.1f}%")
                    print(f"  RAM: {ram_percent:.1f}%") 
                    print(f"  DISCO: {disco_percent:.1f}%")

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
                    
                    cursor.execute(queryConsumo, (fk_maquina, potencia, 1))
                    db.commit()
                    
                    print(f"Consumo Energia inserindo: {potencia:.2f} W")

                    io_before_total = p.disk_io_counters(perdisk=False)
                    time.sleep(1)
                    io_after_total = p.disk_io_counters(perdisk=False)

                    taxaLeitura = (io_after_total.read_bytes - io_before_total.read_bytes) / 1024 / 1024
                    taxaEscrita = (io_after_total.write_bytes - io_before_total.write_bytes) / 1024 / 1024

                    processos = []
                    for proc in p.process_iter(['name', 'memory_info']):
                        try:
                            uso = proc.info['memory_info'].rss / 1024 / 1024
                            processos.append((proc.info['name'], uso))
                        except:
                            pass

                    processos.sort(key=lambda x: x[1], reverse=True)
                    top1, top2, top3 = processos[:3]

                    processosLeitura = []
                    processosEscrita = []

                    for proc in p.process_iter(['name', 'io_counters']):
                        try:
                            io = proc.info['io_counters']
                            if io:
                                processosLeitura.append((proc.info['name'], io.read_bytes))
                                processosEscrita.append((proc.info['name'], io.write_bytes))
                        except:
                            pass

                    processosLeitura.sort(key=lambda x: x[1], reverse=True)
                    processosEscrita.sort(key=lambda x: x[1], reverse=True)

                    procMaisLeitura, procMaisLeituraValor = processosLeitura[0]
                    procMaisEscrita, procMaisEscritaValor = processosEscrita[0]

                    queryRegistro = """INSERT INTO Registro(captura, fkComponente) VALUES (%s, %s)"""
                    cursor.execute(queryRegistro, (cpu_percent, fk_comp1))
                    db.commit()

                    cursor.execute(queryRegistro, (ram_percent, fk_comp2))
                    db.commit()

                    cursor.execute(queryRegistro, (disco_percent, fk_comp3))
                    db.commit()

                    query = """
                        INSERT INTO RegistroDisco
                        (fkMaquina, taxaLeitura, taxaEscrita,
                        top1, top1Valor,
                        top2, top2Valor,
                        top3, top3Valor,
                        procMaisLeitura, procMaisLeituraValor,
                        procMaisEscrita, procMaisEscritaValor)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """


                    cursor.execute(query, (
                        fk_maquina,
                        taxaLeitura, taxaEscrita,
                        top1[0], top1[1],
                        top2[0], top2[1],
                        top3[0], top3[1],
                        procMaisLeitura, procMaisLeituraValor,
                        procMaisEscrita, procMaisEscritaValor
                    ))
                    db.commit()

                    print("RegistroDisco inserido")

            cursor.close()
            db.close()

    except Error as e:
        print('Error to connect with MySQL -', e)

    except KeyboardInterrupt:
        print(f"\nParando...")

insercao()