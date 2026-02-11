import asyncio
from playwright.async_api import async_playwright

URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"
DELAY_S = 2          # delay tra click (NON download)
LOG_EVERY = 10       # stampa stato ogni N pagine

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(URL, wait_until="networkidle")

        # Age gate
        if await page.locator("text=Are you 18 years of age or older?").count() > 0:
            await page.locator("text=Yes").first.click()
            await page.wait_for_load_state("networkidle")

        total_files = 0
        pages = 0

        while True:
            pages += 1

            # conta file nella pagina corrente
            file_links = await page.locator("a[href*='EFTA']").count()
            total_files += file_links

            if pages % LOG_EVERY == 0:
                print(f"Pagine: {pages} | File stimati: {total_files}")

            # trova "Next"
            next_btn = page.locator("text=Next")
            if await next_btn.count() == 0:
                break

            # se disabilitato, fine
            cls = await next_btn.first.get_attribute("class") or ""
            if "disabled" in cls.lower():
                break

            await asyncio.sleep(DELAY_S)
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle")

        print("\n=== RISULTATO FINALE ===")
        print(f"Pagine totali: {pages}")
        print(f"File totali stimati: {total_files}")

        await browser.close()

asyncio.run(main())
