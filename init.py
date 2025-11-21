import asyncio
import aiomysql
import json
from slack_sdk.web.async_client import AsyncWebClient

with open("config.json") as f:
    cfg = json.load(f)

EMPRESA = cfg["empresa"]
MAQUINA = cfg["maquina"]
COMPONENTES = cfg["componentes"]

slack = AsyncWebClient(token=cfg["token"])

async def get_db():
    return await aiomysql.connect(
        host=cfg["db"]["host"],
        user=cfg["db"]["user"],
        password=cfg["db"]["password"],
        db=cfg["db"]["database"],
        port=cfg["db"]["port"]
    )


async def buscar_alertas_novos():
    conn = await get_db()
    cur = await conn.cursor(aiomysql.DictCursor)

    sql = """
        SELECT idAlerta, fkComponente, estado, descricao, dtHora
        FROM Alerta
        WHERE dtHora >= NOW() - INTERVAL 15 SECOND
    """

    await cur.execute(sql)
    alertas = await cur.fetchall()

    await cur.close()
    conn.close()
    return alertas



async def monitorar():
    while True:
        alertas = await buscar_alertas_novos()

        for alerta in alertas:
            comp_id = str(alerta["fkComponente"])
            nome_componente = COMPONENTES.get(comp_id, "Desconhecido")

            msg = (
                f"*ALERTA!* \n"
                f"*Empresa:* {EMPRESA}\n"
                f"*Máquina:* {MAQUINA}\n"
                f"*Componente:* {nome_componente}\n\n"
                f"*Estado:* {alerta['estado']}\n"
                f"*Descrição:* {alerta['descricao']}\n"
                f"{alerta['dtHora']}"
            )

            await slack.chat_postMessage(
                channel=cfg["slack_channel"],
                text=msg
            )

        await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(monitorar())
