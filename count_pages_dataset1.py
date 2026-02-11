import asyncio
import re
from playwright.async_api import async_playwright

URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(URL, wait_until="networkidle")

        # age gate
        if await page.locator("text=Are you 18 years of age or older?").count() > 0:
            await page.locator("text=Yes").first.click()
            await page.wait_for_load_state("networkidle")

        # prova 1: cerca testo tipo "Page X of Y"
        body = await page.locator("body").inner_text()
        m = re.search(r"Page\s+\d+\s+of\s+(\d+)", body, re.IGNORECASE)
        if m:
            pages = int(m.group(1))
            print(f"Pagine totali: {pages}")
            print(f"Stima file: ~{pages * 50}")
            await browser.close()
            return

        # prova 2: guarda il pager numerico (ultimo numero)
        pager_links = await page.locator("ul.pager li a").all_inner_texts()
        nums = [int(x) for x in pager_links if x.isdigit()]
        if nums:
            pages = max(nums)
            print(f"Pagine totali (pager): {pages}")
            print(f"Stima file: ~{pages * 50}")
            await browser.close()
            return

        print("Impossibile determinare il numero di pagine automaticamente.")
        await browser.close()

asyncio.run(main())
