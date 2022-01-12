from bs4 import BeautifulSoup as bs
import aiohttp

async def match_tomorrow() -> list:
    '''
    Function to scrap and return tomorrow's matches
    '''
    async with aiohttp.ClientSession() as session:
        async with session.get("https://sports.news.naver.com/kbaseball/index.nhn") as resp:
            ns_prefix = "https://sports.news.naver.com"
            text = await resp.text()
            soup = bs(text, "lxml")
            games = soup.find_all("li", {"class": "hmb_list_items"})
            res = []
            for i in games:
                temp, etc = {}, []
                for j in i.findChildren():
                    if j.name == 'em':
                        temp['start_time'] = j.text.replace("\n","").replace(chr(32),"")
                    elif j.name == 'a':
                        if j.get('href').startswith('http://'):
                            temp[j.text.replace("\n","").replace(chr(32),"")] = j.get('href')
                        else:
                            if "javascript" in  j.get('href'):
                                temp[j.text.replace("\n","").replace(chr(32),"")] = ns_prefix + j.get('href').split("'")[1]
                            else:
                                temp[j.text.replace("\n","").replace(chr(32),"")] = ns_prefix + j.get('href')

                    elif j.name == "div":
                        if j.get("class")[0] == "score":
                            if not temp.get("score", False):
                                temp['score'] = j.get_text().replace("\n", "").replace(chr(32), "").replace("\t", "")
                            else:
                                temp['score'] = [temp.get("score", False)] + [j.get_text().replace("\n", "").replace(chr(32), "").replace("\t", "")]
                    else:
                        if len(j.get_text().replace("\n", "").replace(chr(32), "")) != 0:
                            etc.append(j.get_text().replace("\n", "").replace(chr(32), "").replace("\t", ""))
                temp['etc'] = etc
                res.append(temp)
    return res

async def match_today() -> list:
    '''
    Function to scrap and return today's matches.
    '''
    ns_prefix = "https://sports.news.naver.com"
    target = "https://sports.news.naver.com/kbaseball/schedule/index.nhn"
    async with aiohttp.ClientSession() as session:
        async with session.get(target) as resp:
            raw = await resp.text()

    soup = bs(raw, "lxml") 
    games = soup.find_all("li", class_="live") + soup.find_all("li", class_="before_game")
    if len(games) == 0:
        games = soup.find_all("li", class_="end")
    #broadcast = a, btn_ltr
    res = []
    for i in games:
        temp, etc = {}, []
        for j in i.findChildren():

            if j.name == 'em':
                temp['start_time'] = j.text.replace("\n","").replace(chr(32),"")
            elif j.name == 'a':
                if j.get('href').startswith('http://'):
                    temp[j.text.replace("\n","").replace(chr(32),"")] = j.get('href')
                else:
                    temp[j.text.replace("\n","").replace(chr(32),"")] = ns_prefix + j.get('href')
            else:
                if len(j.get_text().replace("\n", "").replace(chr(32), "")) != 0:
                    etc.append(j.get_text().replace("\n", "").replace(chr(32), ""))
        temp['etc'] = etc
        res.append(temp)

    return res