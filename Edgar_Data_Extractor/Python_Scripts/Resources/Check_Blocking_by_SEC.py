import httpx

headers = {
    "User-Agent": "Mithun Thakkar (thakkarmithun26@gmail.com)",  # Replace with your details
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

r = httpx.get("https://www.sec.gov", headers=headers)
print(r.status_code)



# Check IP Address
import requests
print(requests.get("https://api64.ipify.org").text)


