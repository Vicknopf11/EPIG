import httpx

TEST_URL = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

with httpx.Client(headers=headers, follow_redirects=False, timeout=30.0) as c:
    r = c.get(TEST_URL)
    print("Status:", r.status_code)
    print("Location:", r.headers.get("location"))

    # se arriva un redirect a age-verify, andiamo lì e stampiamo i Set-Cookie
    loc = r.headers.get("location", "")
    if r.status_code in (301,302,303,307,308) and loc:
        r2 = c.get(loc, follow_redirects=False)
        print("\n--- Age verify page ---")
        print("Age status:", r2.status_code)
        print("Set-Cookie headers:")
        for k, v in r2.headers.items():
            if k.lower() == "set-cookie":
                print(v)
        print("\nCookies in jar now:", c.cookies)
