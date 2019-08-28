import requests, json, os, sys, time, math
from datetime import datetime


def split_list(lst_long,n):
    lst_splitted = []
    totalBatches = math.ceil(len(lst_long) / n)
    for i in range(totalBatches):
        lst_short = lst_long[i*n:(i+1)*n]
        lst_splitted.append(lst_short)
    return lst_splitted



def show_work_status(singleCount, totalCount, currentCount=0):
    if totalCount > 0:
        currentCount += singleCount
        percentage = 100.0 * currentCount / totalCount
        status =  '>' * int(percentage)  + ' ' * (100 - int(percentage))
        sys.stdout.write('\r[{0}] {1:.2f}% '.format(status, percentage))
        sys.stdout.flush()
        if percentage >= 100:
            print('\n')



def get_steam_app_info():
    url = 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
    r = requests.get(url)
    dic_app_list = r.json()
    lst_app_id = [i.get('appid') for i in dic_app_list.get('applist').get('apps')]
    print('Total apps:', len(lst_app_id))

    total_count = len(lst_app_id)
    current_count = 0
    show_work_status(0, total_count, current_count)

    path_app_detail_sample = 'app_detail.txt' 
    with open(path_app_detail_sample, 'w') as f:
        for app_id in lst_app_id:
            url_app_detail = ('http://store.steampowered.com/api/appdetails?appids=%s') % (app_id)
            for i in range(3):
                try:
                    r = requests.get(url_app_detail)
                    result = r.json()
                    break
                except:
                    time.sleep(5)
                    pass
            f.write(json.dumps(result))
            f.write('\n')
            show_work_status(1, total_count, current_count)
            current_count += 1
            if current_count % 200 == 0:
                time.sleep(300)
            else:
                time.sleep(.5)



get_steam_app_info()






