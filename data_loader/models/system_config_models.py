from pydantic.dataclasses import dataclass

@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str

@dataclass
class ServerConfig:
    host: str
    port: int

@dataclass
class Config:
    server: ServerConfig
    database: DatabaseConfig