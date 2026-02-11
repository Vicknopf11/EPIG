import asyncio
import httpx

BASE = "https://www.justice.gov/epstein/files/DataSet%201"
# Esempio noto di file valido (dal sito DOJ)
# https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf

def efta_url(n: int) -> str:
    return f"{BASE}/EFTA{n:08d}.pdf"

async def exists(client: httpx.AsyncClient, url: str) -> bool:
    # HEAD è più leggero; se non supportato, fallback a GET range
    r = await client.head(url, follow_redirects=False)
    if r.status_code == 200:
        return True
    # age-verify può ritornare 302 verso /age-verify
    if r.status_code in (301, 302, 303, 307, 308):
        loc = r.headers.get("location", "")
        if "age-verify" in loc:
            # file esiste ma c'è gate; consideriamo "esiste"
            return True
    if r.status_code == 405:  # HEAD not allowed
        r2 = await client.get(url, headers={"Range": "bytes=0-0"}, follow_redirects=False)
        if r2.status_code in (200, 206):
            return True
        if r2.status_code in (301, 302, 303, 307, 308) and "age-verify" in (r2.headers.get("location","")):
            return True
    return False

async def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        # 1) verifica che il file 1 esista
        u1 = efta_url(1)
        ok1 = await exists(client, u1)
        print("EFTA00000001 exists?", ok1, u1)
        if not ok1:
            print("Blocco/WAF o URL cambiato. Fermati qui.")
            return

        # 2) trova un upper bound raddoppiando
        lo = 1
        hi = 1
        while True:
            hi *= 2
            if hi > 50_000_000:  # safety cap
                break
            if await exists(client, efta_url(hi)):
                lo = hi
                print("Upper bound expand: exists", hi)
                await asyncio.sleep(0.2)
                continue
            else:
                print("Upper bound found: missing", hi)
                break

        # 3) binary search tra lo (esiste) e hi (manca)
        left, right = lo, hi
        while left + 1 < right:
            mid = (left + right) // 2
            if await exists(client, efta_url(mid)):
                left = mid
            else:
                right = mid
            # micro delay per non martellare
            await asyncio.sleep(0.05)

        print("\n=== RESULT ===")
        print("Last existing (approx, may be gaps):", left)
        print("Next missing:", right)
        print("Last URL:", efta_url(left))

if __name__ == "__main__":
    asyncio.run(main())
