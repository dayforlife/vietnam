import asyncio
import os

import aiohttp
import pandas as pd

# Cookies
cookies = {
    '_cfuvid': 'eGjybcPw3OPEHHnR.IznZ9dyPlmX2BpnmnLWvE1xF.Y-1735050256360-0.0.1.1-604800000',
    '_ga': 'GA1.1.1066005476.1735050257',
    '_hjHasCachedUserAttributes': 'true',
    '__uidac': '01676ac4130c193576048d647b8121bd',
    '__admUTMtime': '1735050259',
    '__iid': '6461',
    '__su': '0',
    '_gcl_au': '1.1.1154180240.1735050264',
    'PH_ONBOARDING_SESSION': '0',
    '_hjSessionUser_1708983': 'eyJpZCI6IjRkNTU1Yjk2LWZlYmItNWIxNy1iZTFhLWRjMzFiMjM0ZWY5ZCIsImNyZWF0ZWQiOjE3MzUwNTAyNTc0NDAsImV4aXN0aW5nIjp0cnVlfQ==',
    'con.unl.lat': '1735059600',
    'con.unl.sc': '2',
    'download-app-cookie': '2',
    'MS_ONBOARDING_SESSION': '0',
    'ajs_anonymous_id': '445bb5f0-f4e7-4bda-8c77-3ccf4d867bd2',
    '_clck': 'z4sj84%7C2%7Cfs0%7C0%7C1819',
    'con.ses.id': '3a342b16-8c20-4e24-920f-52c0169cf324',
    '_hjSession_1708983': 'eyJpZCI6IjA1NzMyMGIwLTUwMzMtNDFhOS04NjAyLTk4NDU1ZjNiOWEwMiIsImMiOjE3MzUxMDkxNTc0MzksInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=',
    'ab.storage.deviceId.892f88ed-1831-42b9-becb-90a189ce90ad': '%7B%22g%22%3A%22e718ab4a-99be-c1df-fb99-fb0acd58935e%22%2C%22c%22%3A1735050258571%2C%22l%22%3A1735109158382%7D',
    'USER_PRODUCT_SEARCH': '38%7C41%7CHN%7C3%7C28%7C393%7C0%2C41918920',
    '.AspNetCore.Antiforgery.VyLW6ORzMgk': 'CfDJ8MWmQygzY9lMmEaYV-iWH9hpGeZBjkbcgad1-lSpJQeMdjCuTKaPTSHSYLqSeL43b7W-SREVF7Pf_6PU0Q6uWMYllfSXJlQItUZxsen1PbmjcolNWlmjubu7uzUgidrfRZTajZLTG8pjdWgvXzkoo6U',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22sGfWWcQiEUgWnm84xOcc%22%2C%22expiryDate%22%3A%222025-12-25T07%3A03%3A59.519Z%22%7D',
    'ab.storage.sessionId.892f88ed-1831-42b9-becb-90a189ce90ad': '%7B%22g%22%3A%22ee873125-08aa-05e0-29c2-d99fe1d7d981%22%2C%22e%22%3A1735112040839%2C%22c%22%3A1735109158381%2C%22l%22%3A1735110240839%7D',
    '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22expiryDate%22%3A%222025-12-25T07%3A04%3A01.023Z%22%7D',
    '_clsk': 'edcgvq%7C1735110241140%7C6%7C1%7Ck.clarity.ms%2Fcollect',
    'con.unl.usr.id': '%7B%22key%22%3A%22userId%22%2C%22value%22%3A%22445bb5f0-f4e7-4bda-8c77-3ccf4d867bd2%22%2C%22expireDate%22%3A%222025-12-25T14%3A15%3A45.4562094Z%22%7D',
    'con.unl.cli.id': '%7B%22key%22%3A%22clientId%22%2C%22value%22%3A%229ab2820e-6a85-4fe6-a60b-2a32873903d1%22%2C%22expireDate%22%3A%222025-12-25T14%3A15%3A45.4562378Z%22%7D',
    '.AspNetCore.Mvc.CookieTempDataProvider': 'CfDJ8MWmQygzY9lMmEaYV-iWH9gV20CCtxWGiKo7aAG8EYAk5s7PGtblXz5tCUwXUiu9shSbESi9iLly1CTjGxuqyQtgOaqEx40rPS39a-5WrKjGlsD9qd5R3D0rc_OMJvD1KkQLHIGo5woqL9QMLMf0akg',
    '_ga_HTS298453C': 'GS1.1.1735109156.4.1.1735110944.60.0.0',
}

# Заголовки
headers = {
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'cookie': '_cfuvid=eGjybcPw3OPEHHnR.IznZ9dyPlmX2BpnmnLWvE1xF.Y-1735050256360-0.0.1.1-604800000; _ga=GA1.1.1066005476.1735050257; _hjHasCachedUserAttributes=true; __uidac=01676ac4130c193576048d647b8121bd; __admUTMtime=1735050259; __iid=6461; __iid=6461; __su=0; __su=0; _gcl_au=1.1.1154180240.1735050264; PH_ONBOARDING_SESSION=0; _hjSessionUser_1708983=eyJpZCI6IjRkNTU1Yjk2LWZlYmItNWIxNy1iZTFhLWRjMzFiMjM0ZWY5ZCIsImNyZWF0ZWQiOjE3MzUwNTAyNTc0NDAsImV4aXN0aW5nIjp0cnVlfQ==; con.unl.lat=1735059600; con.unl.sc=2; download-app-cookie=2; MS_ONBOARDING_SESSION=0; ajs_anonymous_id=445bb5f0-f4e7-4bda-8c77-3ccf4d867bd2; _clck=z4sj84%7C2%7Cfs0%7C0%7C1819; con.ses.id=3a342b16-8c20-4e24-920f-52c0169cf324; _hjSession_1708983=eyJpZCI6IjA1NzMyMGIwLTUwMzMtNDFhOS04NjAyLTk4NDU1ZjNiOWEwMiIsImMiOjE3MzUxMDkxNTc0MzksInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; ab.storage.deviceId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%22e718ab4a-99be-c1df-fb99-fb0acd58935e%22%2C%22c%22%3A1735050258571%2C%22l%22%3A1735109158382%7D; USER_PRODUCT_SEARCH=38%7C41%7CHN%7C3%7C28%7C393%7C0%2C41918920; .AspNetCore.Antiforgery.VyLW6ORzMgk=...; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22sGfWWcQiEUgWnm84xOcc%22%2C%22expiryDate%22%3A%222025-12-25T07%3A03%3A59.519Z%22%7D; ab.storage.sessionId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%22ee873125-08aa-05e0-29c2-d99fe1d7d981%22%2C%22e%22%3A1735112040839%2C%22c%22%3A1735109158381%2C%22l%22%3A1735110240839%7D; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22expiryDate%22%3A%222025-12-25T07%3A04%3A01.023Z%22%7D; _clsk=edcgvq%7C1735110241140%7C6%7C1%7Ck.clarity.ms%2Fcollect; con.unl.usr.id=%7B%22key%22%3A%22userId%22%2C%22value%22%3A%22445bb5f0-f4e7-4bda-8c77-3ccf4d867bd2%22%2C%22expireDate%22%3A%222025-12-25T14%3A15%3A45.4562094Z%22%7D; con.unl.cli.id=%7B%22key%22%3A%22clientId%22%2C%22value%22%3A%229ab2820e-6a85-4fe6-a60b-2a32873903d1%22%2C%22expireDate%22%3A%222025-12-25T14%3A15%3A45.4562378Z%22%7D; .AspNetCore.Mvc.CookieTempDataProvider=CfDJ8MWmQygzY9lMmEaYV-iWH9gV20CCtxWGiKo7aAG8EYAk5s7PGtblXz5tCUwXUiu9shSbESi9iLly1CTjGxuqyQtgOaqEx40rPS39a-5WrKjGlsD9qd5R3D0rc_OMJvD1KkQLHIGo5woqL9QMLMf0akg; _ga_HTS298453C=GS1.1.1735109156.4.1.1735110944.60.0.0',
    'sec-ch-ua': '"Not A(Brand";v="99", "Chromium";v="118", "Google Chrome";v="118"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'referer': 'https://batdongsan.com.vn/ban-can-ho-chung-cu?cIds=650,362,41,325,163,575,361,40,283,44,562,45,48',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}


async def get_page(incr: int, df, sem):
    async with sem:
        if incr < len(df):
            async with aiohttp.ClientSession() as session:
                try:
                    url = df.iloc[incr].iloc[-1]
                    async with session.get(url) as response:
                        html = await response.text()
                        print(f'Парсим страницу: №{incr}\nURL: {url}')

                        await get_data(html, url)

                        await asyncio.sleep(1)
                except aiohttp.ClientConnectionError as e:
                    print(f'Ошибка соедиениния с {url}: {e}')
        else:
            print('Вышли за пределы DataFrame')

async def main():
    df = pd.read_csv(os.path.join('data', 'links.csv'))
    sem = asyncio.Semaphore(15)

    tasks = [get_page(incr=i, df=df, sem=sem) for i in range(len(df))]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())