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
    seller_id: str


@dataclass
class SheetsConfig:
    """Google Sheets configuration."""
    spreadsheet_id: str
    master_tab_name: str
    credentials_path: str


@dataclass
class Thresholds:
    """Analysis thresholds for quarterly tracker."""
    # Rank Status thresholds (impression share %)
    rank_top_3_threshold: float = 20.0
    rank_page_1_high_threshold: float = 10.0
    rank_page_1_low_threshold: float = 1.0

    # Diagnostic thresholds for opportunity score
    ghost_min_volume: int = 500
    ghost_max_imp_share: float = 1.0
    window_shopper_min_imp_share: float = 10.0
    window_shopper_max_click_share: float = 1.0
    price_problem_min_imp_share: float = 5.0


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
            seller_id=config("SELLER_ID", default=""),
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
            # Rank Status thresholds
            rank_top_3_threshold=config(
                "RANK_TOP_3_THRESHOLD", default=20.0, cast=float
            ),
            rank_page_1_high_threshold=config(
                "RANK_PAGE_1_HIGH_THRESHOLD", default=10.0, cast=float
            ),
            rank_page_1_low_threshold=config(
                "RANK_PAGE_1_LOW_THRESHOLD", default=1.0, cast=float
            ),
            # Diagnostic thresholds
            ghost_min_volume=config(
                "GHOST_MIN_VOLUME", default=500, cast=int
            ),
            ghost_max_imp_share=config(
                "GHOST_MAX_IMP_SHARE", default=1.0, cast=float
            ),
            window_shopper_min_imp_share=config(
                "WINDOW_SHOPPER_MIN_IMP_SHARE", default=10.0, cast=float
            ),
            window_shopper_max_click_share=config(
                "WINDOW_SHOPPER_MAX_CLICK_SHARE", default=1.0, cast=float
            ),
            price_problem_min_imp_share=config(
                "PRICE_PROBLEM_MIN_IMP_SHARE", default=5.0, cast=float
            ),
        ),
    )
