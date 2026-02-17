import asyncio
from playwright.async_api import async_playwright

URL = "https://www.justice.gov/epstein"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(URL, wait_until="domcontentloaded")

        print("\n➡️  Completa manualmente eventuali banner/consensi/login nel browser.")
        print("➡️  Quando vedi che riesci ad aprire un PDF di DataSet 1 SENZA redirect, torna qui.")
        input("\nPremi INVIO per salvare i cookie (storage_state) e chiudere... ")

        await context.storage_state(path="justice_storage.json")
        print("\n✅ Salvato: justice_storage.json")
        await browser.close()

asyncio.run(main())
