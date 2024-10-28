from dataclasses import dataclass

@dataclass
class Game:
    appid: int
    name: str
    ranking: int

@dataclass
class TagName:
    tag_name: str

@dataclass
class TagId:
    tag_id: int

@dataclass
class TagsOfGames:
    appid: int
    tag_id: int

@dataclass
class AppId:
    appid: int

@dataclass
class GameName:
    name: str

@dataclass
class Ranking:
    ranking: int