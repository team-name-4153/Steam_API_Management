
from flask import Flask, request, jsonify, url_for
from apscheduler.schedulers.background import BackgroundScheduler
from database.rds_database import rds_database
from models import Steam_API_Management_Model
from dataclasses import asdict
# from util import *
import requests
import logging
import time
import os
from dotenv import load_dotenv


# Setup
load_dotenv()
BASEDIR = os.path.abspath(os.path.dirname(__file__))
DB_NAME = os.getenv("RDS_DB_NAME")
STEAM_TOP_100_API = os.getenv("STEAM_TOP_100_API")
STEAM_GAME_DETAIL_API = os.getenv("STEAM_GAME_DETAIL_API")
cur_database = rds_database(db_name=DB_NAME)
app = Flask(__name__)
# Configure logging
logging.basicConfig(level=logging.INFO)


# Middleware for logging
@app.before_request
def log_request():
    request.start_time = time.time() # Record the start time
    request_data = request.get_data() # Get request data if needed
    app.logger.info(f'Before Request: Method={request.method}, Path={request.path}, Body={request_data.decode()}')


@app.after_request
def log_response(response):
    duration = time.time() - request.start_time # Calculate how long the request took
    app.logger.info(f'After Request: Method={request.method}, Path={request.path}, Status={response.status_code}, Duration={duration:.2f} sec')
    return response


# Routes
@app.route('/')
def index():
    return jsonify({"message": "Welcome to Steam API Management"}), 200


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
    try:
        game_info = cur_database.query_data('games', columns=['appid', 'name', 'ranking'], conditions=asdict(Steam_API_Management_Model.AppId(appid=gameId)))[0]
        if not game_info:
            return jsonify({"message": "Game not found"}), 404
        tag_id_info = cur_database.query_data('tags_of_games', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.AppId(appid=gameId)))
        tag_id_info = [tag_id['tag_id'] for tag_id in tag_id_info]
        tag_name_info = []
        for tag_id in tag_id_info:
            tag_name = cur_database.query_data('game_tags', columns=['tag_name'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))[0]['tag_name']
            tag_name_info.append(tag_name)
        game_info['tags'] = tag_name_info

        # HATEOS
        response = {
            "game_detail": game_info,
            "_links": {
                "self": url_for('request_game_detail_by_id', gameId=gameId, _external=True),
                "top_100_game_list_query_example": url_for('request_game_list',page=1, per_page=10, _external=True),
                "game_list_by_tag_query_example": url_for('request_game_list_by_tag', tagName=tag_name_info[0].replace(" ", "%20"),page=1, per_page=10, _external=True) if tag_name_info else None
            }
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"message": "Failed to fetch game detail"}), 500


"""
Request game detail by game name (use %20 to replace space in game name)

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
@app.route("/steam_api/game_detail/<string:gameName>", methods=['GET'])
def request_game_detail_by_name(gameName):
    try:
        gameName = gameName.replace("%20", " ")
        game_info = cur_database.query_data('games', columns=['appid', 'name', 'ranking'], conditions=asdict(Steam_API_Management_Model.GameName(name=gameName)))[0]
        if not game_info:
            return jsonify({"message": "Game not found"}), 404
        tag_id_info = cur_database.query_data('tags_of_games', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.AppId(appid=game_info['appid'])))
        tag_id_info = [tag_id['tag_id'] for tag_id in tag_id_info]
        tag_name_info = []
        for tag_id in tag_id_info:
            tag_name = cur_database.query_data('game_tags', columns=['tag_name'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))[0]['tag_name']
            tag_name_info.append(tag_name)
        game_info['tags'] = tag_name_info

        # HATEOS
        response = {
            "game_detail": game_info,
            "_links": {
                "self": url_for('request_game_detail_by_name', gameName=gameName, _external=True),
                "top_100_game_list_query_example": url_for('request_game_list', page=1, per_page=10, _external=True),
                "game_list_by_tag_query_example": url_for('request_game_list_by_tag', tagName=tag_name_info[0].replace(" ", "%20"), page=1, per_page=10, _external=True) if tag_name_info else None
            }
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"message": "Failed to fetch game detail"}), 500


"""
Request game list by tag name (use %20 to replace space in tag name)

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
        ...
        1174180,
        1238810,
        1240440,
        1517290,
        1938090
    ]
    """
@app.route('/steam_api/game_list_by_tag/<string:tagName>',methods=['GET'])
def request_game_list_by_tag(tagName):
    try:
        tagName = tagName.replace("%20", " ")
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        tag_id = cur_database.query_data('game_tags', columns=['tag_id'], conditions=asdict(Steam_API_Management_Model.TagName(tag_name=tagName)))[0]['tag_id']
        if not tag_id:
            return jsonify({"message": "Tag not found"}), 404
        game_id_info = cur_database.query_data('tags_of_games', columns=['appid'], conditions=asdict(Steam_API_Management_Model.TagId(tag_id=tag_id)))
        game_id_info = [game_id['appid'] for game_id in game_id_info]

        # pagination
        start = (page - 1) * per_page
        end = page * per_page
        paginated_game_id_info = game_id_info[start:end]

        # HATEOS
        response = {
            "game_list_by_tag": paginated_game_id_info,
            "_links": {
                "self": url_for('request_game_list_by_tag', tagName=tagName, page=page, per_page=per_page, _external=True),
                "next": url_for('request_game_list_by_tag', tagName=tagName, page=page + 1, per_page=per_page, _external=True) if end < len(game_id_info) else None,
                "prev": url_for('request_game_list_by_tag', tagName=tagName, page=page - 1, per_page=per_page, _external=True) if start > 0 else None,
                "game_detail_query_example": url_for('request_game_detail_by_id', gameId=paginated_game_id_info[0], _external=True) if paginated_game_id_info else None
            }
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"message": "Failed to fetch game list by tag"}), 500


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
        ...
        96000,
        632360,
        1046930,
        275850,
        322170
    ]
"""
@app.route('/steam_api/game_list',methods=['GET'])
def request_game_list():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        game_list = cur_database.query_top_100_game()
        game_list = [game['appid'] for game in game_list]

        # pagination
        start = (page - 1) * per_page
        end = page * per_page
        paginated_game_list = game_list[start:end]

        # HATEOS
        response = {
            "game_list": paginated_game_list,
            "_links": {
                "self": url_for('request_game_list', page=page, per_page=per_page, _external=True),
                "next": url_for('request_game_list', page=page + 1, per_page=per_page, _external=True) if end < len(game_list) else None,
                "prev": url_for('request_game_list', page=page - 1, per_page=per_page, _external=True) if start > 0 else None,
                "game_detail_query_example": url_for('request_game_detail_by_id', gameId=paginated_game_list[0], _external=True) if paginated_game_list else None
            }
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"message": "Failed to fetch top 100 games"}), 500


@app.errorhandler(404)
def page_not_found(e):
    return jsonify({"message": "Page not found"}), 404

def fetch_steam_api_data():
    print("Fetching top 100 games data from Steam API")
    try:
        response = requests.get(STEAM_TOP_100_API)
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
                    game_detail_response = requests.get(STEAM_GAME_DETAIL_API + str(game))
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