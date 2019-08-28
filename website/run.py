from flask import Flask, render_template
import random, json
from sqlalchemy import create_engine

app = Flask(__name__)


path_steam_user_id = 'steam_user_id.txt'


username = ''
password = ''
host = '127.0.0.1'
database = 'game_recommendation'
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(username, password, host, database))


with open(path_steam_user_id,'r') as f:
	lst_user_id = f.readlines()


dic_valid_games = {}
for app_id,name,initial_price,header_image in engine.execute('''
	SELECT 
		MIN(app_id),
		name,
		initial_price,
		header_image 
	FROM 
		game_steam_app 
	WHERE 
		type = "game" 
		AND 
		release_date <= CURDATE() 
		AND initial_price IS NOT NULL
	GROUP BY
		name, initial_price, header_image
	''').fetchall():
	dic_valid_games[app_id] = (app_id,name,float(initial_price),header_image)


lst_popular_games = []
for i in engine.execute('SELECT app_id FROM game_steam_app_owner ORDER BY owners DESC LIMIT 5').fetchall():
	app_id = i[0]
	if app_id in dic_valid_games:
		lst_popular_games.append(dic_valid_games.get(app_id))



@app.route('/')
def recommender():
	userid = random.choice(lst_user_id)

	# userid = 76561197960323774 # no purchase info

	lst_most_played_games = []
	for i in engine.execute('SELECT app_id FROM game_steam_user_inventory WHERE user_id = {} AND playtime_forever > 0 ORDER BY playtime_forever DESC LIMIT 3'.format(userid)).fetchall():
		app_id = i[0]
		if app_id in dic_valid_games:
			lst_most_played_games.append(dic_valid_games.get(app_id))


	lst_content_recommended = []
	lst_item_recommended = []
	lst_als_recommended = []

	if lst_most_played_games:
		favorite_app_id = lst_most_played_games[0][0]

		# get content based recommendation
		for app_id in engine.execute('SELECT `0`,`1`,`2`,`3`,`4` FROM recommended_games_content_based WHERE app_id = {}'.format(favorite_app_id)).first():
			if app_id in dic_valid_games:
				lst_content_recommended.append(dic_valid_games.get(app_id))

		# get item based recommendation
		for app_id in engine.execute('SELECT `0`,`1`,`2`,`3`,`4` FROM recommended_games_item_based WHERE app_id = {}'.format(favorite_app_id)).first():
			if app_id in dic_valid_games:
				lst_item_recommended.append(dic_valid_games.get(app_id))


		# get ALS based recommendation
		for app_id in engine.execute('SELECT `0`,`1`,`2`,`3`,`4` FROM recommended_games_als_based WHERE user_id = {}'.format(userid)).first():
			if app_id in dic_valid_games:
				lst_als_recommended.append(dic_valid_games.get(app_id))



	return render_template( 'recommendation.html',
							userid = userid,
							lst_most_played_games = lst_most_played_games,
							lst_content_recommended = lst_content_recommended,
							lst_item_recommended = lst_item_recommended,
							lst_als_recommended = lst_als_recommended,
							lst_popular_games = lst_popular_games)


if __name__ == '__main__':
	app.run(debug=True)



