import asyncio
import httpx
from urllib.parse import urlparse

BASE = "https://www.justice.gov/epstein/files/DataSet%201"

def efta_url(n: int) -> str:
    return f"{BASE}/EFTA{n:08d}.pdf"

def is_pdf_response(r: httpx.Response) -> bool:
    ct = (r.headers.get("content-type") or "").lower()
    return "pdf" in ct

def is_age_verify_url(url: str) -> bool:
    try:
        return "/age-verify" in urlparse(url).path
    except Exception:
        return False

async def warmup_age_gate(client: httpx.AsyncClient, sample_pdf_url: str) -> None:
    # Trigger redirect to age-verify and load it to obtain QueueIT cookie (if any)
    r = await client.get(sample_pdf_url, follow_redirects=False)
    if r.status_code in (301, 302, 303, 307, 308):
        loc = r.headers.get("location", "")
        if loc and "age-verify" in loc:
            await client.get(loc, follow_redirects=True)

async def exists_pdf(client: httpx.AsyncClient, url: str) -> bool:
    # Lightweight fetch; follow redirects, but accept only real PDF responses
    r = await client.get(url, headers={"Range": "bytes=0-1"}, follow_redirects=True)

    # If we ended on age-verify, it's NOT a confirmed PDF
    if is_age_verify_url(str(r.url)):
        return False

    if r.status_code in (200, 206) and is_pdf_response(r):
        return True

    # 404 is definitive
    if r.status_code == 404:
        return False

    # Other statuses: treat as not exists (could be throttling; handle upstream)
    return False

async def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        # Warm up cookie/protection
        await warmup_age_gate(client, efta_url(1))

        # Sanity check: #1 should be a real PDF
        ok1 = await exists_pdf(client, efta_url(1))
        print("EFTA00000001 confirmed PDF?", ok1)
        if not ok1:
            print("Non riesco a ottenere un PDF reale. Possibile blocco o serve cookie diverso.")
            return

        # Find an upper bound by doubling until missing
        lo = 1
        hi = 1

        # Cap: scegli una soglia ragionevole per non impazzire
        CAP = 5_000_000

        while True:
            hi *= 2
            if hi > CAP:
                print(f"Raggiunto CAP={CAP} senza trovare missing. Fermando espansione.")
                break

            if await exists_pdf(client, efta_url(hi)):
                lo = hi
                print("Upper bound expand: exists", hi)
            else:
                print("Upper bound found: missing", hi)
                break

            await asyncio.sleep(0.2)

        # If we never found missing within CAP, stop here
        if hi > CAP and lo == hi // 2:
            print("Non ho trovato un limite superiore entro CAP. Aumenta CAP se serve.")
            print("Last confirmed within CAP:", lo)
            print("URL:", efta_url(lo))
            return

        # Binary search in (lo, hi)
        left, right = lo, hi
        while left + 1 < right:
            mid = (left + right) // 2
            if await exists_pdf(client, efta_url(mid)):
                left = mid
            else:
                right = mid
            await asyncio.sleep(0.05)

        print("\n=== RESULT ===")
        print("Last confirmed existing ID:", left)
        print("First confirmed missing ID:", right)
        print("Last URL:", efta_url(left))

if __name__ == "__main__":
    asyncio.run(main())
