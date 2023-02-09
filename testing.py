import re
import requests
import os
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import pandas as pd


def threads_con(function, Dev, session, headers, list_attach, limit=1):
    func = partial(function, session, headers, list_attach)
    with ThreadPoolExecutor(max_workers=limit) as executor:
        f_result = executor.map(func, Dev, timeout=15)
    return list(f_result)


def main():
    list_attach = {}
    try:
        login_txt = open(os.getcwd() + '\\login.txt', 'r')
        login_text = login_txt.readline().split(';')
        login = str(login_text[0])
        password = str(login_text[1])
    except:
        print('Файл с логином и паролем пуст!')

    try:
        session = requests.Session()
        session.auth = HttpNtlmAuth(login, password, session)
        headers = {'Content-Type': 'text/html; charset=utf-8', 'Content-Encoding': 'gzip',
                   'Accept-Encoding': 'identity'}
        login = session.get('https://rdb.tele2.ru/p/list.aspx?op=list&k=c3a5t2r&v=c3a5ts5c2cs9r142',
                            headers=headers)  # логинимся rdb
        print(login)
    except:
        print('Что-то не так с логином и паролем')
    cand = session.get('https://rdb.tele2.ru/p/list.aspx?op=list&k=c3a5t20r&view=c3a5ts5c20cs7r3003&page=1&pagesize=80',
                       headers=headers)
    text = cand.content
    # print(cand.text)
    # print(text)
    # print(zlib.decompress(cand.content, 16+zlib.MAX_WBITS))

    html = text
    soup = BeautifulSoup(html, 'html.parser')
    paging = soup.find(lambda tag: tag.name == 'div' and tag.get('class') == ['LayoutPaging'])
    print(paging, '##############')
    if paging != None:
        last = paging.findAll(lambda tag: tag.name == 'a' and tag.attrs['href'])
        links = [link.attrs['href'] for link in last]
        numb_page = re.findall(r'&page=(.*?)&', str(links[-1]))
        numb_page = int(numb_page[0])
    else:
        numb_page = 1
    print(numb_page, 'numb_page')

    for i in range(1, numb_page + 1):  # numb_page-10, numb_page
        print('pageeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', i)
        page_with_attach = session.get(
            'https://rdb.tele2.ru/p/list.aspx?op=list&k=c3a5t20r&v=c3a5ts5c20cs7r3003&page=' + str(i) + '&pagesize=80',
            headers=headers)
        page_with_attach = BeautifulSoup(page_with_attach.content, 'html.parser')
        attach = page_with_attach.findAll(
            lambda tag: tag.name == 'tr' and (tag.get('class') == ['Odd'] or tag.get('class') == ['Even']))
        # attach=attach.findAll(lambda tag: tag.name == 'td' and tag.get('class') == ['fieldType-eDocument mainField'])
        # print(attach)
        for tr in attach:
            # print('\n\n\n\'',tr)
            link_a = re.search(r't:c3a5t20r_(.*?)">', str(tr)).group()
            link_a = link_a.replace('">', '').split('_')[1]
            # print(link_a)
            # print('form&amp;k='+link_a+'>')
            link_name = re.search('form&amp;k=' + link_a + r'">(.*?)</a', str(tr)).group()
            link_name = link_name.replace('form&amp;k=' + link_a + '">', '').replace('</a', '')
            try:
                link_kand = re.search(r'fs13(.*?)</a', str(tr)).group()
            except:
                link_kand = 'Unknown'
            # print('YA TYTTTTT',link_kand, '\n\n\n\n')
            if link_kand != 'Unknown':
                link_kand1 = str(link_kand.split('>')[-1]).replace('</a', '')

                link_kand2 = re.search(r'b1f2(.*?)</', str(tr)).group()
                if str(link_kand2.split('>')[1])[0] == '<':
                    link_kand2 = 'Report'
                else:
                    link_kand2 = str(link_kand2.split('>')[-1]).replace('</', '')
                date_otchet = re.search(r'eDateTime(.*?)</', str(tr)).group()
                date_otchet = (date_otchet.split('>')[-1]).replace('</', '')
                # print(link_a, link_name, link_kand1, link_kand2, date_otchet)
                dict_l = dict(name=link_name, kand=link_kand1, type=link_kand2, date=date_otchet)
                dict_main = {}
                dict_main[link_a] = dict_l
                # dict_for_parse=dict(%s: '{'name':%s, 'kand': %s, 'type': %s}' % ("'"+str(link_a)+"'", str(link_name), str(link_kand1), str(link_kand2)))

                # dict_for_parse = json.dumps(dict_for_parse)
                # dict_for_parse=json.loads(dict_for_parse)
                # print(dict_for_parse)
                list_attach.update(dict_main)
            else:
                continue

    # to_download(session, headers, list_attach)
    # filelist = [ f for f in os.listdir('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\') if f.endswith(".pdf") ]
    # print(filelist)
    # for f in filelist:
    #	os.remove(os.path.join('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\'+f))
    path = '\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\'

    ######БЛОК УДАЛЕНИЯ ВСЕХ ФАЙЛОВ В КОРНЕ
    '''for file in os.scandir(path):
        try:
            if file.name.endswith(".pdf"):
                os.unlink(file.path)
        except:
            continue

    print('Файлы удалены')	
    '''
    ##########################################

    r = open('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + 'result_download.csv',
             'w')
    r.write('Кандидат,Имя файла,тип,дата изменения\n')
    r.close()
    z = open('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + 'result_download.txt',
             'w', encoding="utf-8")
    z.write('Kand,Name,Type,date\n')
    z.close()

    # t=threads_con(download_files,list_attach.keys(),session, headers, list_attach, limit=1)
    for key in list_attach.keys():
        download_files(session, headers, list_attach, key)


# df = pd.read_csv('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\'+'result_download.txt', encoding='latin-1')
# df.to_csv('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\'+'output.csv', sep=',', index=False, encoding='latin-1')

#	sample = """{ "configuration": { "ssh": { "access": %s, "login": %s, "password": %s } },"server": {"host":%s,"port": %s}} """  % ("true", "some", "some", "127.0.01", "22")
# print(tr)
# tr=BeautifulSoup(str(tr), 'html.parser')
# id_link=tr.findAll(lambda tag: tag.name == 'td' and tag.get('class') == ['fieldType-eDocument mainField'] )

# list_attach=np.append(list_attach, tr.findAll(lambda tag: tag.name == 'td' and tag.get('class') == ['fieldType-eDocument mainField'] ))
# print(list_attach)
# print(id_link)
def to_download(session, headers, list_attach):
    t = threads_con(download_files, list_attach.keys(), limit=1)


def download_files(session, headers, list_attach, link):
    # print(link)
    # print(str(list_attach[link]['name']), str(list_attach[link]['type']))
    # print(list_attach)
    # print('Поехали грузить')
    if str(list_attach[link]['name'].split('.')[-1]) == 'pdf' and str(list_attach[link]['type']) == 'Report':
        download = session.get('https://rdb.tele2.ru/p/document.aspx?op=document&k=' + link + 'b1f1', headers=headers,
                               stream=True)
        # print(download)
        # print(download.headers)
        download.headers.get('Content-Disposition')
        filename = '\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + str(
            list_attach[link]['name'])
        open(filename, 'wb').write(download.content)
        r = open(
            '\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + 'result_download.csv',
            'a')
        r.write(list_attach[link]['kand'] + ',' + list_attach[link]['name'] + ',' + list_attach[link]['type'] + ',' +
                list_attach[link]['date'] + '\n')
        l = open('\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + 'log.txt', 'a')
        l.write(list_attach[link]['kand'] + ',' + list_attach[link]['name'] + ',' + list_attach[link]['type'] + ',' +
                list_attach[link]['date'] + '\n')
        z = open(
            '\\\T2RU-DCS-02.corp.tele2.ru\\CPFolders\\Transport_planning\\Reports_RRL_RDB\\' + 'result_download.txt',
            'a')
        z.write(list_attach[link]['kand'] + ',' + list_attach[link]['name'] + ',' + list_attach[link]['type'] + ',' +
                list_attach[link]['date'] + '\n')

        l.close()
        r.close()
        z.close()
        # print('Кандидат: {} Вложение: {} Type: {} Дата: {}'.format((list_attach[link]['kand'], 'NoData')+','+(list_attach[link]['name'],'NoData')+','+(list_attach[link]['type'], 'NoData')+','+(list_attach[link]['date'], 'NoData')))

        print('\nЗагрузил ', str(list_attach[link]['name']))

    '''obj = soup.findAll(lambda tag: tag.name == 'input' and tag.get('class') == ['groupOperation'])
    links = [link.attrs['value'] for link in obj]
    for link in links:
        link_cand='https://rdb.tele2.ru/p/form.aspx?op=form&k='+link
        link_otchet=session.get(link_cand, headers=headers)
        kand=link_otchet.content
        soupv2 = BeautifulSoup(kand, 'html.parser')
        #print(soupv2.attrs)
        objv2 = soupv2.findAll(lambda tag: tag.name == 'tr' and tag.get('class') == ['Odd'])
        #print(objv2)
        link_topdf=re.findall(r't:c3a5t20r_(.*?)">\n<td><a', str(objv2))
        download=session.get('https://rdb.tele2.ru/p/document.aspx?op=document&k='+str(link_topdf[0])+'b1f1', headers=headers, stream=True)
        print(download)
        print(download.headers)
        download.headers.get('Content-Disposition')
        filename = os.getcwd()+'\\'+str(download.headers.get('Content-Disposition')).split('"')[1]
        print(filename)
        open(filename, 'wb').write(download.content)
        #p/document.aspx?op=document&amp;k=c3a5t20r8677076b1f1
        #https://rdb.tele2.ru/p/form.aspx?op=form&k=c3a5t20r8677076
        print(link_topdf)
        print('download')'''


# print(links)
# print(cand.text)
# print(soup.find("td", class_="CustomAction").text)
# print(re.findall(r'<table>(.*?)</table>', text))
# print(login.text.xpath('tbody z="t:c3a5t2r"'))
# print(table_cand)
# print(cand.text)

if __name__ == '__main__':
    file = 'create.xlsx'
    main()

    a = print('THE END, файлы загружены')






