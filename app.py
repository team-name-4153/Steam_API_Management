
from flask import Flask, render_template, Response, send_from_directory, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from database.rds_database import rds_database
from models import Steam_API_Management_Model
from dataclasses import asdict
import Const
from util import *
import requests
import os
import sys
from dotenv import load_dotenv
load_dotenv()
DB_NAME = os.getenv("RDS_DB_NAME")



BASEDIR = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__)
cur_database = rds_database(db_name=DB_NAME)


"""
Request game detail by game id

Returns: dict of game info

Example:
    /steam_api/game_detail/730
    Response:
    {
        "appid": 730,
        "name": "Counter-Strike: Global Offensive",
        "ranking": 2,
        "tags": [
            "FPS",
            "Shooter",
            "Multiplayer",
            "Competitive",
            "Action",
            "Team-Based",
            "e-sports",
            "Tactical",
            "First-Person",
            "PvP",
            "Strategy",
            "Online Co-Op",
            "Difficult",
            "Co-op",
            "Military",
            "War",
            "Trading",
            "Realistic",
            "Fast-Paced",
            "Moddable"
        ]
    }
"""
@app.route("/steam_api/game_detail/<int:gameId>", methods=['GET'])
def request_game_detail_by_id(gameId):
    game_info = cur_database.query_data('games', columns=['appid', 'name', 'ranking'], conditions=asdict(Steam_API_Management_Model.AppId(appid=gameId)))[0]
    tag_id_info = cur_database.query_data('tags_of_games', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.AppId(appid=gameId)))
    tag_id_info = [tag_id['tag_id'] for tag_id in tag_id_info]
    tag_name_info = []
    for tag_id in tag_id_info:
        tag_name = cur_database.query_data('game_tags', columns=['tag_name'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))[0]['tag_name']
        tag_name_info.append(tag_name)
    game_info['tags'] = tag_name_info
    return jsonify(game_info)


"""
Request game detail by game name

Returns: dict of game info

Example:
    /steam_api/game_detail/Counter-Strike
    Response:
    {
        {
            "appid": 10,
            "name": "Counter-Strike",
            "ranking": 46,
            "tags": [
                "FPS",
                "Shooter",
                "Multiplayer",
                "Competitive",
                "Action",
                "Team-Based",
                "e-sports",
                "Tactical",
                "First-Person",
                "PvP",
                "Strategy",
                "Military",
                "Survival",
                "Classic",
                "1990's",
                "Old School",
                "Score Attack",
                "1980s",
                "Assassin",
                "Nostalgia"
            ]
        }
    }
"""
# TODO: fix name with space issue
@app.route("/steam_api/game_detail/<string:gameName>", methods=['GET'])
def request_game_detail_by_name(gameName):
    game_info = cur_database.query_data('games', columns=['appid', 'name', 'ranking'], conditions=asdict(Steam_API_Management_Model.GameName(name=gameName)))[0]
    tag_id_info = cur_database.query_data('tags_of_games', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.AppId(appid=game_info['appid'])))
    tag_id_info = [tag_id['tag_id'] for tag_id in tag_id_info]
    tag_name_info = []
    for tag_id in tag_id_info:
        tag_name = cur_database.query_data('game_tags', columns=['tag_name'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))[0]['tag_name']
        tag_name_info.append(tag_name)
    game_info['tags'] = tag_name_info
    return jsonify(game_info)


"""
Request game list by tag name

Returns:
    list: list of game id

Example:
    /steam_api/game_list_by_tag/FPS
    Response:
    [
        10,
        70,
        240,
        320,
        340,
        400,
        440,
        550,
        620,
        730,
        4000,
        49520,
        107410,
        218230,
        218620,
        221100,
        239140,
        251570,
        252490,
        275850,
        291480,
        304930,
        359550,
        377160,
        433850,
        444090,
        550650,
        578080,
        594650,
        755790,
        1085660,
        1091500,
        1172470,
        1174180,
        1238810,
        1240440,
        1517290,
        1938090
    ]
    """
# TODO: fix tag name with space issue
@app.route('/steam_api/game_list_by_tag/<string:tagName>',methods=['GET'])
def request_game_list_by_tag(tagName):
    tag_id = cur_database.query_data('game_tags', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.TagName(tag_name=tagName)))[0]['tag_id']
    game_id_info = cur_database.query_data('tags_of_games', columns=['appid'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))
    game_id_info = [game_id['appid'] for game_id in game_id_info]
    return jsonify(game_id_info)


"""
Request top 100 game list

Returns: list of game id

Example:
    /steam_api/game_list
    Response:
    [
        570,
        730,
        578080,
        440,
        1172470,
        1623730,
        1063730,
        2358720,
        1938090,
        271590,
        550,
        252490,
        1599340,
        304930,
        553850,
        236390,
        1245620,
        105600,
        291550,
        431960,
        1086940,
        4000,
        359550,
        1085660,
        340,
        346110,
        230410,
        892970,
        1091500,
        945360,
        218620,
        901583,
        238960,
        413150,
        242760,
        1203220,
        899770,
        381210,
        1097150,
        292030,
        291480,
        49520,
        444090,
        438100,
        227300,
        10,
        272060,
        739630,
        620,
        990080,
        252950,
        1966720,
        1240440,
        1517290,
        70,
        582010,
        240,
        108600,
        320,
        386360,
        1468810,
        648800,
        755790,
        1174180,
        550650,
        400,
        301520,
        239140,
        367520,
        250900,
        814380,
        433850,
        251570,
        1222670,
        594650,
        322330,
        219990,
        261550,
        304050,
        236850,
        532210,
        377160,
        477160,
        107410,
        255710,
        289070,
        1811260,
        218230,
        1238810,
        72850,
        221100,
        1089350,
        204360,
        1326470,
        394360,
        96000,
        632360,
        1046930,
        275850,
        322170
    ]
"""
@app.route('/steam_api/game_list',methods=['GET'])
def request_game_list():
    game_list = cur_database.query_top_100_game()
    game_list = [game['appid'] for game in game_list]
    return jsonify(game_list)


def fetch_steam_api_data():
    print("Fetching top 100 games data from Steam API")
    try:
        response = requests.get(Const.steamTop100API)
        if response.status_code == 200:
            data = response.json()
            # print(data)
            # set rank to 101 for all games for upcoming update
            result = cur_database.update_data("games", asdict(Steam_API_Management_Model.Ranking(ranking=101)), {})
            if result != "Success":
                print("Failed to reset rank", result)
                return
            # fetch game detail data for each game from Steam API
            ranking = 0
            for game in data:
                ranking += 1
                # print(ranking)
                try:
                    game_detail_response = requests.get(Const.steamGameDetailAPI + str(game))
                    if game_detail_response.status_code == 200:
                        # insert new game into database
                        game_detail = game_detail_response.json()
                        game_exist = cur_database.check_data_exist("games", asdict(Steam_API_Management_Model.AppId(appid=game_detail['appid'])))
                        if game_exist:
                            print("game exist. update rank")
                            game_insert_result = cur_database.update_data("games", asdict(Steam_API_Management_Model.Ranking(ranking=ranking)), asdict(Steam_API_Management_Model.AppId(appid=game_detail['appid'])))
                        else:
                            print("game not exist. insert new game")
                            game_insert_result = cur_database.bulk_insert_data("games", [asdict(Steam_API_Management_Model.Game(appid=game_detail['appid'], name=game_detail['name'], ranking=ranking))])          
                        if game_insert_result != "Success":
                            print("Failed to update rank:", game_insert_result)
                            return
                            
                        # insert new tags into database
                        tags = game_detail['tags']
                        tags_data = []
                        for tag in tags.keys():
                            tag_exist = cur_database.check_data_exist("game_tags", asdict(Steam_API_Management_Model.TagName(tag_name=tag)))
                            if not tag_exist:
                                tags_data.append(asdict(Steam_API_Management_Model.TagName(tag_name=tag)))

                        if tags_data:
                            tag_insert_result = cur_database.bulk_insert_data("game_tags", tags_data)
                            if tag_insert_result != "Success":
                                print("Failed to insert new tags:", tag_insert_result)
                                return

                        # update game-tag relationship
                        game_tag_relationships = []
                        for tag in tags.keys():
                            tag_id = cur_database.query_data("game_tags", ['tag_id'], asdict(Steam_API_Management_Model.TagName(tag_name=tag)))[0]['tag_id']
                            game_tag_relationship_exist = cur_database.check_data_exist("tags_of_games", asdict(Steam_API_Management_Model.TagsOfGames(appid=game_detail['appid'], tag_id=tag_id)))
                            if not game_tag_relationship_exist:
                                game_tag_relationships.append(asdict(Steam_API_Management_Model.TagsOfGames(appid=game_detail['appid'], tag_id=tag_id)))
                        
                        if game_tag_relationships:
                            game_tag_relationship_insert_result = cur_database.bulk_insert_data("tags_of_games", game_tag_relationships)
                            if game_tag_relationship_insert_result != "Success":
                                print("Failed to insert game-tag relationship:", game_tag_relationship_insert_result)
                                return
                except Exception as e:
                    print("Failed to fetch game detail data from Steam API")
                    print(e)
        else:
            print("Failed to fetch top 100 games data from Steam API", response.status_code)
    except Exception as e:
        print("Failed to fetch top 100 games data from Steam API")
        print(e)
            

if __name__ == '__main__':
    # TODO is scheduler running?
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_steam_api_data, 'interval', weeks=2)
    scheduler.start()
    app.run(debug=True, port=5000, host='0.0.0.0')