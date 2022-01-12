from bs4 import BeautifulSoup as bs
from pathlib import Path
import re
import aiosqlite
import aiohttp
import time
import datetime
import json

ROOT = Path(__file__)
DBPATH = Path("Directory to the db")

async def standing() -> str:
    '''
    Return current league standing
    '''
    async with aiosqlite.connect(DBPATH) as db:
        async with db.execute("SELECT * FROM Etcdata WHERE name='standing' LIMIT 1") as cursor:
            entry = await cursor.fetchone()
            if entry[2] != time.strftime('%Y-%m-%d %H'):
                async with aiohttp.ClientSession() as session:
                    async with session.get("http://www.koreabaseball.com/TeamRank/TeamRank.aspx") as kbohtml:
                        html = await kbohtml.text()
                        await db.execute("UPDATE Etcdata SET last_checked='{}', cache='{}' WHERE name='standing'".format(time.strftime('%Y-%m-%d %H'), html.replace(chr(39),"")))
                        await db.commit()
            else:
                html = entry[3]

    soup = bs(html, 'lxml')
    res = "        Team    W   L   D   Win%\n" \
          + "    ================================="
    i = 1
    while (i <= 10):
        res += "\n"
        base = soup.findAll("tr")[i]

        for j,k in enumerate(base.findAll("td")[0:7]):
            text = k.get_text()
            if j == 1:
                res += "{:>5}".format(text)
            elif j in [0,3,4,5]:
                res += "{:>5}".format(text)
            elif j == 6:
                res += "{:>8}".format(text)

        i += 1
    return res

async def stat_leaders(**kwargs) -> str:
    '''
    fetch leaderboard of each stat
    '''
    kbocategory = {'b_avg': "/Record/Player/HitterBasic/Basic1.aspx?sort=HRA_RT", 'hr': "/Record/Player/HitterBasic/Basic1.aspx?sort=HR_CN"
            , 'rbi': "/Record/Player/HitterBasic/Basic1.aspx?sort=RBI_CN", 'sb': "/Record/Player/Runner/Basic.aspx?sort=SB_CN"
            , 'r': "/Record/Player/HitterBasic/Basic1.aspx?sort=RUN_CN", 'h': "/Record/Player/HitterBasic/Basic1.aspx?sort=HIT_CN"
            , 'obp': "/Record/Player/HitterBasic/Basic2.aspx?sort=OBP_RT", 'slg': "/Record/Player/HitterBasic/Basic2.aspx?sort=SLG_RT"
            , '2b': "/Record/Player/HitterBasic/Basic1.aspx?sort=H2_CN", '3b': "/Record/Player/HitterBasic/Basic1.aspx?sort=H3_CN"
            , 'tb': "/Record/Player/HitterBasic/Basic1.aspx?sort=TB_CN", 'ops': "/Record/Player/HitterBasic/Basic2.aspx?sort=OPS_RT"
            , 'ab': "/Record/Player/HitterBasic/Basic1.aspx?sort=AB_CN", 'bb': "/Record/Player/HitterBasic/Basic2.aspx?sort=BB_CN"
            , 'k': "/Record/Player/HitterBasic/Basic2.aspx?sort=KK_CN", 'dp': "/Record/Player/HitterBasic/Basic2.aspx?sort=GD_CN"
            , 'bb': "/Record/Player/HitterBasic/Basic2.aspx?sort=HP_CN", 'ibb': "/Record/Player/HitterBasic/Basic2.aspx?sort=IB_CN"
            , 'mh': "/Record/Player/HitterBasic/Basic2.aspx?sort=MH_HITTER_CN"
            , 'err': "/Record/Player/Defense/Basic.aspx?sort=ERR_CN", 'cs': "/Record/Player/Defense/Basic.aspx?sort=CS_RT"
            , 'pb': "/Record/Player/Defense/Basic.aspx?sort=PB_CN"
            , 'era': "/Record/Player/PitcherBasic/Basic1.aspx?sort=ERA_RT"
            , 'win': "/Record/Player/PitcherBasic/Basic1.aspx?sort=W_CN", 'sv': "/Record/Player/PitcherBasic/Basic1.aspx?sort=SV_CN"
            , 'win': "/Record/Player/PitcherBasic/Basic1.aspx?sort=WRA_RT", 'hld': "/Record/Player/PitcherBasic/Basic1.aspx?sort=HOLD_CN"
            , 'p_k': "/Record/Player/PitcherBasic/Basic1.aspx?sort=KK_CN", 'game': "/Record/Player/PitcherBasic/Basic1.aspx?sort=GAME_CN"
            , 'l': "/Record/Player/PitcherBasic/Basic1.aspx?sort=L_CN", 'inn': "/Record/Player/PitcherBasic/Basic1.aspx?sort=INN2_CN"
            , 'p_bb': "/Record/Player/PitcherBasic/Basic1.aspx?sort=BB_CN", 'whip': "/Record/Player/PitcherBasic/Basic1.aspx?sort=WHIP_RT"
            , 'cg': "/Record/Player/PitcherBasic/Basic2.aspx?sort=CG_CN", 'sho': "/Record/Player/PitcherBasic/Basic2.aspx?sort=SHO_CN"
            , 'qs': "/Record/Player/PitcherBasic/Basic2.aspx?sort=QS_CN", 'ovag': "/Record/Player/PitcherBasic/Basic2.aspx?sort=OAVG_RT"
            }

    if kwargs['category'] not in kbocategory:
        return None

    async with aiosqlite.connect(DBPATH) as db:
        async with db.execute("SELECT * FROM Etcdata WHERE name=':category' LIMIT 1", {'category': kwargs['category']}) as cursor:
            entry = await cursor.fetchone()
            if entry[2] != time.strftime('%Y-%m-%d %H'):
                async with aiohttp.ClientSession() as kboses:
                    async with kboses.get(f"https://www.koreabaseball.com/{kbocategory.get(kwargs['category'], '')}") as kbohtml:
                        html = await kbohtml.text()
                        await db.execute("UPDATE Etcdata SET last_checked='{}', cache='{}' WHERE name='{}'"\
                                        .format(time.strftime('%Y-%m-%d %H'), html.replace(chr(39),""), kwargs['category']))
                        await db.commit()
            else:
                html = entry[3]

    soup = bs(html, 'lxml')
    key = kbocategory.get(kwargs["category"]).split(chr(61))[1]
    res = []
    for i in soup.find_all("tr")[1:11]:
        temp = []
        childs = i.findChildren()
        temp.extend(list(map(lambda x: x.get_text(), childs[0:4])))
        for j in childs:
            if j.attrs.get("data-id", None) == key:
                temp.append(j.get_text())
        res.append(temp)

    wrapped = "{:^30}\n".format(kwargs['category'] + "랭킹")
    wrapped += "{rank>4} {name>5} {team>8} {:category>5}\n====================================\n", {'category': kwargs['category']})
    for k in res:
        wrapped += "{:>4} {:>6} {:>6} {:>7}\n".format(k[0], k[1].rjust(5, "ㅤ"), k[3], k[4])

    return wrapped

async def get_plyrs_db_count(name=str) -> int:
    async with aiosqlite.connect(DBPATH) as db:
        async with db.execute("SELECT COUNT(*) FROM Players WHERE name=:player_name", {"player_name": name}) as cursor:
            player_cnt = await cursor.fetchone()
    
    return player_cnt[0]

async def get_plyrs_web_data(name:str) -> list:
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://www.koreabaseball.com/Player/Search.aspx?searchWord={name}') as kbocs:
            kbohtml = await kbocs.text()
            soup = bs(kbohtml, 'lxml')

    return soup.find('tbody').find_all('tr')

async def bb_player(name:str) -> list:
    async with aiosqlite.connect(DBPATH) as db:
        if await get_plyrs_db_count(name) != len(await get_plyrs_web_data(name)):
            await bb_kakasi(name=name)
        async with db.execute("SELECT * FROM Players WHERE name=:player_name", {"player_name": name}) as cursor:
            cursor = await db.execute("SELECT * FROM Players WHERE name=:player_name", {"player_name": name})
            kbotgt = await cursor.fetchall()

        res = []
        res_for_update = []
        for i in kbotgt:
            if i[5] == time.strftime('%Y-%m-%d'):
                temp = json.loads(i[6])
            else:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://www.koreabaseball.com/Record/Player/{i[3]}Detail/Basic.aspx?playerId={i[2]}") as resp:
                        html = await resp.text()

                    soup = bs(html, 'lxml')
                    stat_title = list(map(lambda x: x.getText().lower(), sum([tr.find_all('th') for tr in soup.find_all('thead', limit=2)], [])))
                    stat_data = list(map(lambda x: x.getText(), sum([tr.find_all('td') for tr in soup.find_all('tbody', limit=2)], [])))
                    temp = {i: j for i, j in zip(stat_title, stat_data)}
                    temp['type'] = i[3]
                    temp['name'] = name
                    temp['backno'] = soup.find(id="cphContents_cphContents_cphContents_playerProfile_lblBackNo").getText()
                    temp['birth'] = soup.find(id="cphContents_cphContents_cphContents_playerProfile_lblBirthday").getText()
                    temp['from_bt'] = divmod((datetime.datetime.now() - datetime.datetime.strptime('-'.join(re.findall(r'\d+', temp['birth'])), "%Y-%m-%d")).days, 365)
                    temp['pos'] = soup.find(id="cphContents_cphContents_cphContents_playerProfile_lblPosition").getText()
                    temp['pic'] = "https:" + soup.find(id="cphContents_cphContents_cphContents_playerProfile_imgProgile")['src']
                    temp['nick'] = i[4]
                    employer = soup.find("h4", {"class": lambda x: x and x.startswith('team regular')})
                    temp['teamname'] = employer.getText() if employer else "No Team"
                    if temp['teamname'] != "Record unavailable":
                        temp['active'] = True
                        if temp['type'] == 'Pitcher':
                            temp['ip_f'] = eval(temp['ip'].replace(chr(32), chr(43)))
                            #FIP (13*HR+3*(HBP+BB)-2*K)/IP, plus a constant (usually around 3.2) to put it on the same scale as earned run average.
                            temp['fip'] = round((13 * int(temp['hr']) + 3 * int(temp['bb']) - 2 * int(temp['so'])) /temp['ip_f'] + 3.3, 2)
                            temp['babip'] = round((int(temp['h'])-int(temp['hr']))/(int(temp['tbf']) - int(temp['sac']) + int(temp['sf']) -int(temp['bb']) - int(temp['ibb'])-int(temp['so'])-int(temp['hr'])),3)
                        else:
                            temp['babip'] = round((int(temp['h'])-int(temp['hr']))/(int(temp['ab']) - int(temp['sac']) + int(temp['sf']) - int(temp['so']) - int(temp['hr'])),3)
                            temp['woba'] = round((0.690 * (int(temp['bb'])-int(temp['ibb'])) + 0.722 * int(temp['hbp']) + 0.888 * (int(temp['h']) - int(temp['2b']) - int(temp['3b']) - int(temp['hr']))\
                                            + 1.271 * int(temp['2b']) + 1.616 * int(temp['3b']) + 2.101 * int(temp['hr'])) / (int(temp['ab']) + int(temp['bb']) - int(temp['ibb'])  + int(temp['sf']) + int(temp['hbp'])),3)
                    else:
                        temp['active'] = False

                    res_for_update.append((i[2], json.dumps(temp)))
            
            res.append(temp)        

        return res, res_for_update

async def bb_kakasi(name:str):
    async with aiosqlite.connect(DBPATH) as db:
        for i in await get_plyrs_web_data(name=name):
            base = list(map(lambda x: x.getText(), i.findChildren()))
            if base[0] == "#":
                continue
            elif base[1] == name:                
                link = i.find('a')['href']
                id_ = link.split("=")[1]
                pos = "Pitcher" if base[4] == "투수" else "Hitter"
                async with db.execute("SELECT COUNT(*) FROM Players WHERE ID=:id", {"id": id_}) as cursor:
                    data_ = await cursor.fetchone()
                if data_[0] >= 1:
                    pass
                else:
                    await db.execute(f"INSERT INTO Players (id, name, type, nick) VALUES ('{id_}', '{base[1]}', '{pos}', '')")

        await db.commit()

async def bb_update_all(bulk:list):
    async with aiosqlite.connect(DBPATH) as db:        
        for i in bulk:
            await db.execute("UPDATE Players SET last_checked=:time, cache=:data WHERE id=:id" 
                            , {"id": i[0], "time": time.strftime('%Y-%m-%d'), "data": i[1]})
        await db.commit()


def tdrem(a):
    return str(a).strip('<td/>')
