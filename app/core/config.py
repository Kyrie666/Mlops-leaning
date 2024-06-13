from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    EXECUTION_ENV: str = "dev"
    PROJECT_NAME: str = "data-ml-server"

    # MSSQL Settings
    HOST: str
    DBUSER: str
    PASSWORD: str
    DATABASE: str
    DATABASE_IMAGE: str
    JSON_URL: str
    # POST Settings
    POST_URL: str
    # ALGORITHM Settings
    SEARCH_CNT: int

    class Config:
        env_file = "./env/.env"


settings = Settings()
