from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = f"sqlite:///{BASE_DIR / 'data' / 'horseracing.db'}"

    # Discord webhooks
    DISCORD_WEBHOOK_URL: str = ""
    DISCORD_PNL_WEBHOOK_URL: str = ""

    # Betting configuration (tuned from 2-season walk-forward backtest)
    INITIAL_BANKROLL: float = 10000.0
    KELLY_FRACTION: float = 0.03  # 3% fractional Kelly
    MAX_BET_PCT: float = 0.02  # Never bet more than 2% of bankroll
    MIN_BET_AMOUNT: float = 10.0  # Skip bets below this

    # Value-bet filters. Sliced the 2.5-year walk-forward output by odds:
    # - 4.5-5.5: +26% ROI in-sample, +11% ROI out-of-sample (Jan-Apr 2026)
    # - 5.5-6.5: loses money in-sample (-16%) — exclude
    # Narrowed BET_MAX_ODDS from 7.0 to 5.5 after OOS validation.
    # Distance filter: 1400-1599m bled money in-sample AND OOS. Exclude.
    PRIMARY_MODEL: str = "xgboost"
    BET_MIN_ODDS: float = 4.5
    BET_MAX_ODDS: float = 5.5
    BET_EDGE_MARGIN: float = 0.20  # model_prob must exceed implied_prob * (1 + 0.20)
    BET_TOP_RANK_ONLY: int = 2  # Only consider top-2 model picks per race
    BET_EXCLUDE_DISTANCE_MIN: int = 1400  # Exclude mid-distance races
    BET_EXCLUDE_DISTANCE_MAX: int = 1599

    # Shadow mode: record predictions + simulated bets but DO NOT place real bets.
    # Use this to validate live performance before risking real money.
    SHADOW_MODE: bool = True

    # HKJC pool deduction rates (take-out)
    WIN_PLACE_DEDUCTION: float = 0.175  # 17.5%
    QUINELLA_DEDUCTION: float = 0.175  # 17.5%
    FORECAST_DEDUCTION: float = 0.195  # 19.5%
    TIERCE_TRIO_DEDUCTION: float = 0.20  # 20%
    FIRST4_QUARTET_DEDUCTION: float = 0.20  # 20%

    # HKJC URLs
    HKJC_BASE_URL: str = "https://racing.hkjc.com/racing/information"
    HKJC_RESULTS_URL: str = (
        "https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx"
    )
    HKJC_RACECARD_URL: str = (
        "https://racing.hkjc.com/racing/information/English/racing/RaceCard.aspx"
    )
    HKJC_ENTRIES_URL: str = (
        "https://racing.hkjc.com/racing/information/English/racing/Entries.aspx"
    )
    HKJC_HORSE_URL: str = (
        "https://racing.hkjc.com/racing/information/english/Horse/Horse.aspx"
    )
    HKJC_FIXTURE_URL: str = (
        "https://racing.hkjc.com/racing/information/English/Racing/Fixture.aspx"
    )
    HKJC_JOCKEY_RANKING_URL: str = (
        "https://racing.hkjc.com/racing/information/English/Jockey/JockeyRanking.aspx"
    )
    HKJC_TRAINER_RANKING_URL: str = (
        "https://racing.hkjc.com/racing/information/English/Trainers/TrainerRanking.aspx"
    )

    # MJC URLs
    MJC_BASE_URL: str = "https://www.mjc.mo"

    # Scraping
    SCRAPE_DELAY: float = 3.0  # Seconds between requests
    SELENIUM_HEADLESS: bool = True
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3

    # Scheduling
    ODDS_POLL_INTERVAL_SECONDS: int = 60
    RESULTS_CHECK_INTERVAL_MINUTES: int = 15

    # Model
    MODEL_DIR: Path = BASE_DIR / "models"
    TRAIN_WINDOW_MONTHS: int = 24
    TEST_WINDOW_MONTHS: int = 1

    # Logging
    LOG_LEVEL: str = "INFO"

    # Data paths
    DATA_DIR: Path = BASE_DIR / "data"
    HISTORICAL_DATA_DIR: Path = BASE_DIR / "data" / "historical"
    NEW_DATA_DIR: Path = BASE_DIR / "data" / "new"
    RAW_DATA_DIR: Path = BASE_DIR / "data" / "raw"
    OUTPUT_DIR: Path = BASE_DIR / "output"
