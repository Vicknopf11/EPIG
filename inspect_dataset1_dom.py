import asyncio
from playwright.async_api import async_playwright

URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"

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

        total_links = await page.locator("a").count()
        pdf_links = await page.locator("a[href$='.pdf']").count()
        any_files = await page.locator("a[href*='/files/']").count()
        rows = await page.locator("table tr").count()
        next_candidates = await page.locator("li[class*='next']").count()

        print("=== DOM INSPECTION ===")
        print(f"Total <a> links: {total_links}")
        print(f"PDF links: {pdf_links}")
        print(f"Links containing '/files/': {any_files}")
        print(f"Table rows: {rows}")
        print(f"Next pager candidates: {next_candidates}")

        await browser.close()

asyncio.run(main())
