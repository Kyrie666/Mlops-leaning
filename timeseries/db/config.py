from pydantic_settings import BaseSettings
from datetime import date


class Settings(BaseSettings):
    ENV: str = "dev"
    PROJECT_NAME: str = "time_series"

    # MSSQL Settings
    HOST: str
    DBUSER: str
    PASSWORD: str
    DATABASE: str
    # Data Settings
    TRAIN_START_DATE: date
    TRAIN_END_DATE: date

    class Config:
        env_file = "./.env"


settings = Settings()
