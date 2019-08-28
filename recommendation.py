import requests, json, os, sys, time, re, math
from bs4 import BeautifulSoup
from datetime import datetime

from sqlalchemy import create_engine, types
import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel,cosine_similarity

from pyspark.ml.recommendation import ALS
from pyspark import SparkContext
from pyspark.sql import SparkSession

def show_work_status(singleCount, totalCount, currentCount=0):
    currentCount += singleCount
    percentage = 100.0 * currentCount / totalCount
    status =  '>' * int(percentage)  + ' ' * (100 - int(percentage))
    sys.stdout.write('\r[{0}] {1:.2f}% '.format(status, percentage))
    sys.stdout.flush()
    if percentage >= 100:
        print('\n')


def split_list(lst_long,n):
    lst_splitted = []
    if len(lst_long) % n == 0:
        totalBatches = len(lst_long) / n
    else:
        totalBatches = len(lst_long) / n + 1
    for i in range(int(totalBatches)):
        lst_short = lst_long[i*n:(i+1)*n]
        lst_splitted.append(lst_short)
    return lst_splitted



# set file path
path_app_info = 'app_detail.txt'
path_user_inventory = 'user_inventory.txt'


# config database connection
username = ''
password = ''
host = '127.0.0.1'
database = 'game_recommendation'
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(username, password, host, database))
jdbc_url = 'jdbc:mysql://{}/{}'.format(host, database)



#####################################
### extract selected app features ###
#####################################

def parse_steam_app_info(steam_app_info):
    if steam_app_info:
        for app_id, app_info in steam_app_info.items():
            success = app_info.get('success')
            if success: 
                success = 1
                app_data = app_info.get('data')
                developers = ', '.join(app_data.get('developers', []))
                if not developers:
                    developers = None
                publishers = ', '.join(app_data.get('publishers', []))
                if not publishers:
                    publishers = None
                name = app_data.get('name')
                required_age = app_data.get('required_age')
                short_description = app_data.get('short_description')
                critic_score = app_data.get('metacritic', {}).get('score')
                app_type = app_data.get('type')
                recommendation = app_data.get('recommendations',{}).get('total')
                header_image = app_data.get('header_image')
                fullgame = app_data.get('fullgame',{}).get('appid')
                supported_languages = app_data.get('supported_languages')
                if supported_languages:
                    supported_languages = supported_languages.replace('<strong>*</strong>', '').replace('<br>languages with full audio support','')
                if app_data.get('is_free') == True:
                    initial_price = 0
                    currency = 'USD'
                else:
                    if app_data.get('price_overview',{}):
                        initial_price = app_data.get('price_overview',{}).get('initial', 0) / 100
                        currency = app_data.get('price_overview',{}).get('currency')
                    else:
                        initial_price = None
                        currency = None
                if app_data.get('platforms',{}).get('linux'):
                    linux = 1
                else:
                    linux = 0
                if app_data.get('platforms',{}).get('mac'):
                    mac = 1
                else:
                    mac = 0
                if app_data.get('platforms',{}).get('windows'):
                    windows = 1
                else:
                    windows = 0
                if app_data.get('release_date',{}).get('coming_soon') == False:
                    release_date = app_data.get('release_date',{}).get('date')
                    if release_date:
                        try:
                            release_date = datetime.strptime(release_date, '%b %d, %Y').date()
                        except Exception as e:
                            try:
                                release_date = datetime.strptime(release_date, '%d %b, %Y').date()
                            except:
                                try:
                                    release_date = datetime.strptime(release_date, '%b %Y').date()
                                except:
                                    release_date = None
                    else:
                        release_date = None
                else:
                    release_date = None
                dic_steam_app = {
                    app_id : {
                        'app_id' : app_id,
                        'currency' : currency,
                        'developers' : developers,
                        'publishers' : publishers,
                        'name' : name,
                        'required_age' : required_age,
                        'short_description' : short_description,
                        'critic_score' : critic_score,
                        'type' : app_type,
                        'recommendation' : recommendation,
                        'header_image' : header_image,
                        'initial_price' : initial_price,
                        'linux' : linux,
                        'mac' : mac,
                        'windows' : windows,
                        'fullgame' : fullgame,
                        'release_date' : release_date,
                        'supported_languages' : supported_languages,
                        'success' : success
                    }
                }
            else:
                dic_steam_app = {app_id : {'app_id' : app_id, 'success' : 0}}
    else:
        dic_steam_app = {}
    return dic_steam_app




def update_steam_game_info():
    print('Parse app info and dump to databse')
    dic_steam_app = {}
    with open(path_app_info, 'rb') as f:
        lst_raw_string = f.readlines()
        total_count = len(lst_raw_string)
        current_count = 0
        for i in lst_raw_string:
            app_info = json.loads(i)
            dic_steam_app.update(parse_steam_app_info(app_info))
            show_work_status(1, total_count, current_count)
            current_count += 1


    df_steam_app = pd.DataFrame.from_dict(dic_steam_app, 'index')
    df_steam_app = df_steam_app.loc[:,['app_id','name', 'release_date', 'type', 'currency', 'initial_price', 'developers', 'publishers', 'required_age', 'linux', 'mac', 'windows', 'fullgame', 'critic_score', 'recommendation', 'supported_languages', 'header_image', 'short_description', 'success']]
    df_steam_app.to_sql('game_steam_app', engine, if_exists='replace', index=False, dtype = {
        'app_id' : types.Integer(),
        'name' : types.String(200),
        'release_date' : types.Date,
        'type' : types.String(50),
        'currency' : types.String(5),
        'initial_price' : types.Float(),
        'developers' : types.String(500),
        'publishers' : types.String(500),
        'required_age' : types.Integer(),
        'linux' : types.Boolean(),
        'mac' : types.Boolean(),
        'windows' : types.Boolean(),
        'fullgame' : types.Integer(),
        'critic_score' : types.Integer(),
        'recommendation' : types.Integer(),
        'supported_languages' : types.String(500),
        'header_image' : types.String(500),
        'short_description' : types.String(1000),
        'success' : types.Boolean()
        })





def add_owner_count():
    # Update estimated owners
    # https://steamspy.com
    print('Create owners table')
    engine.execute('CREATE TABLE IF NOT EXISTS game_steam_app_owner (app_id INT(11), owner INT(11))')
    engine.execute(
        '''
        INSERT INTO game_steam_app_owner VALUES 
            (570, 117309000),
            (578080, 61095000),
            (440, 45101000),
            (730, 43740000),
            (304930, 33008000),
            (230410, 25690000),
            (550, 19505000),
            (444090, 18586000),
            (227940, 16620000),
            (218620, 15510000)
        ''')




####################################
#### Most Played Games Per User ####
####################################

def get_user_playtime():
    print('Parse user inventory and dump to databse')
    lst_player_game_playtime = []
    with open(path_user_inventory, 'r') as f:
        for raw_string in f.readlines():
            user_id, lst_inventory = list(json.loads(raw_string).items())[0]
            if lst_inventory:
                for i in lst_inventory:
                    app_id = i.get('appid')
                    playtime_forever = i.get('playtime_forever', 0)
                    playtime_2weeks = i.get('playtime_2weeks', 0)
                    lst_player_game_playtime.append('("{}", "{}", "{}", "{}")'.format(user_id, app_id, playtime_forever, playtime_2weeks))


    engine.execute('CREATE TABLE IF NOT EXISTS game_steam_user_inventory (user_id BIGINT(20), app_id INT(11), playtime_forever INT(11), playtime_2weeks INT(11))')

    if lst_player_game_playtime:
        for i in split_list(lst_player_game_playtime, 5000):
            engine.execute(
            '''
                INSERT INTO game_steam_user_inventory (user_id, app_id, playtime_forever, playtime_2weeks) 
                VALUES {} 
                ON DUPLICATE KEY 
                UPDATE playtime_forever = VALUES(playtime_forever), playtime_2weeks = VALUES(playtime_2weeks)
            '''.format(
                    ','.join(i),
                ).replace('"None"','null')
            )






#####################################
#### Build Recommendation Models ####
#####################################


# Model 1: Content based - Description
def recommendation_content_based():
    print('Content Based Model')
    df_game_description = pd.read_sql_query('''SELECT app_id, short_description FROM game_steam_app WHERE short_description IS NOT NULL AND type = "game" AND release_date <= CURDATE() AND initial_price IS NOT NULL''', engine)
    tfidf = TfidfVectorizer(strip_accents='unicode',stop_words='english').fit_transform(df_game_description.short_description.tolist())

    lst_app_id = df_game_description.app_id.tolist()
    dic_recomended = {}
    total_count = df_game_description.shape[0]
    current_count = 0
    for row_index in range(tfidf.shape[0]):
        cosine_similarities = linear_kernel(tfidf[row_index:row_index+1], tfidf).flatten()
        top_related_rows = cosine_similarities.argsort()[-2:-22:-1]
        dic_recomended.update({lst_app_id[row_index]:[lst_app_id[i] for i in top_related_rows]})
        show_work_status(1,total_count,current_count)
        current_count+=1


    df_content_based_results = pd.DataFrame.from_dict(dic_recomended, 'index')
    df_content_based_results.index.name = 'app_id'
    df_content_based_results.reset_index(inplace=True)
    df_content_based_results.to_sql('recommended_games_content_based',engine,if_exists='replace', index = False)



# Model 2: item based
def recommendation_item_based():
    print('Item Based Model')
    dic_purchase = {}
    set_valid_app_id = set([i[0] for i in engine.execute('SELECT app_id FROM game_steam_app WHERE short_description IS NOT NULL AND type = "game"AND release_date <= CURDATE() AND initial_price IS NOT NULL').fetchall()])
    for app_id, user_id in engine.execute('SELECT app_id, user_id FROM game_steam_user_inventory').fetchall():
        if app_id in set_valid_app_id:
            if user_id in dic_purchase:
                dic_purchase[user_id].update({app_id : 1})
            else:
                dic_purchase[user_id] = {app_id : 1}

    df_purchase = pd.DataFrame(dic_purchase).fillna(0)
    purchase_matrix = df_purchase.values
    lst_app_id = df_purchase.index

    total_count = purchase_matrix.shape[0]
    current_count = 0

    dic_recomended_item_based = {}
    for index in range(total_count):
        cosine_similarities = linear_kernel(purchase_matrix[index:index+1], purchase_matrix).flatten()
        lst_related_app = np.argsort(-cosine_similarities)[1:101]
        dic_recomended_item_based.update({lst_app_id[index]:[lst_app_id[i] for i in lst_related_app]})
        show_work_status(1,total_count,current_count)
        current_count+=1


    df_item_based_result = pd.DataFrame.from_dict(dic_recomended_item_based, 'index')
    df_item_based_result.index.name = 'app_id'
    df_item_based_result.reset_index(inplace=True)
    df_item_based_result.to_sql('recommended_games_item_based', engine, if_exists='replace', chunksize = 1000, index = False)



# Model 3: Collaborative Filtering
# NOTE: This model requires PySpark

def recommendation_als_based():
    print('ALS Model')
    sc = SparkContext()
    spark = SparkSession(sc)

    spark.read.format("jdbc").option("url", jdbc_url)\
                .option("user", username).option("password", password)\
                .option("dbtable", "game_steam_user_inventory")\
                .load().createOrReplaceTempView('user_inventory')


    spark.read.format("jdbc").option("url", jdbc_url)\
                .option("user", username).option("password", password)\
                .option("dbtable", "game_steam_app")\
                .load().createOrReplaceTempView('game_steam_app')


    df_user_playtime = spark.sql('SELECT DENSE_RANK() OVER (ORDER BY user_id) AS user, user_id, app_id AS item, playtime_forever AS rating FROM user_inventory WHERE playtime_forever > 0')
    df_valid_games = spark.sql('SELECT app_id FROM game_steam_app WHERE short_description IS NOT NULL AND type = "game" AND initial_price IS NOT NULL')
    df_user_inventory = df_user_playtime.join(df_valid_games, df_user_playtime['item'] == df_valid_games['app_id'], 'inner').select('user','user_id','item','rating')

    dic_real_user_id = df_user_inventory.select('user','user_id').toPandas().set_index('user')['user_id'].to_dict()
    als = ALS(rank = 5)
    model = als.fit(df_user_inventory)
    recommended_games = model.recommendForAllUsers(10)
    dic_recomended_als_based = {}
    for user, lst_recommended_games in recommended_games.select('user', 'recommendations.item').toPandas().set_index('user')['item'].to_dict().items():
        user_id = dic_real_user_id.get(user)
        dic_recomended_als_based[user_id] = {}
        for i, app_id in enumerate(lst_recommended_games):
            dic_recomended_als_based[user_id].update({i:app_id})


    df_als_based_result = pd.DataFrame.from_dict(dic_recomended_als_based, 'index')
    df_als_based_result.index.name = 'user_id'
    df_als_based_result.reset_index(inplace=True)
    df_als_based_result.to_sql('recommended_games_als_based', engine, if_exists='replace', chunksize = 1000, index = False)




def recommend_games():
    update_steam_game_info()
    get_user_playtime()
    recommendation_content_based()
    recommendation_item_based()
    recommendation_als_based()
    print('finish')




recommend_games()





