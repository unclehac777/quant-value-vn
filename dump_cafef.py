import asyncio
import httpx
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

async def test_cafef():
    url = "https://s.cafef.vn/bao-cao-tai-chinh/AAA/IncSta/2026/0/0/0/ket-qua-hoat-dong-kinh-doanh-.chn"
    print(f"Fetching {url}")
    async with httpx.AsyncClient(headers=_HEADERS, follow_redirects=True) as client:
        r = await client.get(url, timeout=15)
        print("Status", r.status_code)
        if r.status_code == 200:
            with open("cafef_response.html", "w") as f:
                f.write(r.text)

asyncio.run(test_cafef())
