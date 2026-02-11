import asyncio
from playwright.async_api import async_playwright

PDF_URL = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"

async def main():
    async with async_playwright() as p:
        # Usa un browser "visibile" e lento abbastanza da non chiudersi subito
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=250,  # rallenta le azioni, utile per debug
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(PDF_URL, wait_until="domcontentloaded")
        print("Aperto URL:", page.url)
        print("\n>>> ORA FAI TU:")
        print("1) Se appare la pagina di verifica età, clicca 'Yes'")
        print("2) Aspetta il redirect (idealmente non deve più essere /age-verify)")
        print("3) Quando vedi che la pagina è cambiata (o parte il download), torna qui e premi INVIO\n")

        # blocca qui finché l’utente non preme invio
        input("Premi INVIO per salvare i cookie e chiudere... ")

        print("URL attuale prima di salvare:", page.url)

        await context.storage_state(path="justice_storage.json")
        print("Salvato: justice_storage.json")

        # opzionale: stampa nomi cookie salvati
        cookies = await context.cookies()
        names = sorted({c["name"] for c in cookies})
        print("Cookie names:", names)

        await browser.close()

asyncio.run(main())
