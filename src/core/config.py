from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Payment Processor"
    api_key: str
    database_url: str
    rabbitmq_url: str
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()