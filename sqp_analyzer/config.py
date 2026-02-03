"""Configuration loader for SQP Analyzer."""

from dataclasses import dataclass
from decouple import config


@dataclass
class SPAPIConfig:
    """SP-API configuration."""
    client_id: str
    client_secret: str
    refresh_token: str
    aws_access_key: str
    aws_secret_key: str
    role_arn: str
    marketplace_id: str


@dataclass
class SheetsConfig:
    """Google Sheets configuration."""
    spreadsheet_id: str
    master_tab_name: str
    credentials_path: str


@dataclass
class Thresholds:
    """Analysis thresholds."""
    bread_butter_min_purchase_share: float
    opportunity_max_imp_share: float
    opportunity_min_purchase_share: float
    leak_min_imp_share: float
    leak_max_click_share: float
    leak_max_purchase_share: float
    price_warning_threshold: float
    price_critical_threshold: float


@dataclass
class AppConfig:
    """Application configuration."""
    sp_api: SPAPIConfig
    sheets: SheetsConfig
    thresholds: Thresholds


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    return AppConfig(
        sp_api=SPAPIConfig(
            client_id=config("SP_API_CLIENT_ID"),
            client_secret=config("SP_API_CLIENT_SECRET"),
            refresh_token=config("SP_API_REFRESH_TOKEN"),
            aws_access_key=config("AWS_ACCESS_KEY"),
            aws_secret_key=config("AWS_SECRET_KEY"),
            role_arn=config("SP_API_ROLE_ARN"),
            marketplace_id=config("MARKETPLACE_ID", default="ATVPDKIKX0DER"),
        ),
        sheets=SheetsConfig(
            spreadsheet_id=config("SPREADSHEET_ID"),
            master_tab_name=config("MASTER_TAB_NAME", default="ASINs"),
            credentials_path=config(
                "GOOGLE_CREDENTIALS_PATH",
                default="google-credentials.json"
            ),
        ),
        thresholds=Thresholds(
            bread_butter_min_purchase_share=config(
                "BREAD_BUTTER_MIN_PURCHASE_SHARE", default=10.0, cast=float
            ),
            opportunity_max_imp_share=config(
                "OPPORTUNITY_MAX_IMP_SHARE", default=5.0, cast=float
            ),
            opportunity_min_purchase_share=config(
                "OPPORTUNITY_MIN_PURCHASE_SHARE", default=5.0, cast=float
            ),
            leak_min_imp_share=config(
                "LEAK_MIN_IMP_SHARE", default=5.0, cast=float
            ),
            leak_max_click_share=config(
                "LEAK_MAX_CLICK_SHARE", default=2.0, cast=float
            ),
            leak_max_purchase_share=config(
                "LEAK_MAX_PURCHASE_SHARE", default=2.0, cast=float
            ),
            price_warning_threshold=config(
                "PRICE_WARNING_THRESHOLD", default=10.0, cast=float
            ),
            price_critical_threshold=config(
                "PRICE_CRITICAL_THRESHOLD", default=20.0, cast=float
            ),
        ),
    )
