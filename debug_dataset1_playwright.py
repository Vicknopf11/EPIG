import asyncio
from playwright.async_api import async_playwright

URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # IMPORTANT: vediamo cosa succede
        context = await browser.new_context(viewport={"width": 1400, "height": 900})
        page = await context.new_page()

        resp = await page.goto(URL, wait_until="domcontentloaded")
        print("HTTP status:", resp.status if resp else None)
        print("URL after goto:", page.url)
        print("Title:", await page.title())

        # prova click age gate (può essere link o button)
        for sel in [
            "text=Are you 18 years of age or older?",
            "text=Yes",
            "button:has-text('Yes')",
            "a:has-text('Yes')",
        ]:
            if await page.locator(sel).count() > 0:
                try:
                    await page.locator(sel).first.click(timeout=1500)
                except:
                    pass

        await page.wait_for_timeout(1500)
        await page.wait_for_load_state("networkidle")

        print("URL after clicks:", page.url)

        html = await page.content()
        print("HTML length:", len(html))

        with open("dataset1_debug.html", "w", encoding="utf-8") as f:
            f.write(html)

        await page.screenshot(path="dataset1_debug.png", full_page=True)

        print("Saved: dataset1_debug.html, dataset1_debug.png")
        await browser.close()

asyncio.run(main())
