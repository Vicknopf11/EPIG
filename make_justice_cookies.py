import asyncio
from playwright.async_api import async_playwright

PDF_URL = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"

async def main():
    async with async_playwright() as p:
        # Usa Chrome "vero" se disponibile (più simile a browser umano)
        # Se fallisce, Playwright userà Chromium.
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(PDF_URL, wait_until="domcontentloaded")

        # Se appare la pagina Age Verification, clicca Yes
        if await page.locator("text=Are you 18 years of age or older?").count() > 0:
            # "Yes" può essere un link o un bottone; proviamo robusto
            if await page.locator("button:has-text('Yes')").count() > 0:
                await page.locator("button:has-text('Yes')").first.click()
            else:
                await page.locator("text=Yes").first.click()

        # Aspetta che finisca redirect/caricamenti
        await page.wait_for_timeout(1500)

        # Salva cookie/sessione
        await context.storage_state(path="justice_storage.json")
        print("Saved storage state to justice_storage.json")

        await browser.close()

asyncio.run(main())
