from datetime import date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Race(Base):
    __tablename__ = "races"
    __table_args__ = (
        UniqueConstraint("race_date", "racecourse", "race_no", name="uq_race_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    racecourse: Mapped[str] = mapped_column(String(10), nullable=False)  # ST, HV, MJC
    race_no: Mapped[int] = mapped_column(Integer, nullable=False)
    race_class: Mapped[str | None] = mapped_column(String(20))  # Class 1-5, Group 1/2/3, Griffin
    distance: Mapped[int | None] = mapped_column(Integer)  # meters
    track_type: Mapped[str | None] = mapped_column(String(20))  # Turf, All Weather
    course_variant: Mapped[str | None] = mapped_column(String(10))  # A, A+3, B, B+2, C, C+3
    going: Mapped[str | None] = mapped_column(String(30))  # Good, Good to Firm, Yielding, etc.
    prize: Mapped[float | None] = mapped_column(Float)  # HKD
    race_name: Mapped[str | None] = mapped_column(String(200))
    finish_time: Mapped[str | None] = mapped_column(String(20))  # e.g. "1:09.52"
    sectional_times: Mapped[dict | None] = mapped_column(JSON)
    field_size: Mapped[int | None] = mapped_column(Integer)  # number of runners
    source: Mapped[str] = mapped_column(String(20), default="hkjc")  # hkjc, mjc, kaggle
    season: Mapped[str | None] = mapped_column(String(10))  # e.g. "2025/26"
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    runners: Mapped[list["Runner"]] = relationship(back_populates="race", cascade="all, delete-orphan")
    dividends: Mapped[list["Dividend"]] = relationship(back_populates="race", cascade="all, delete-orphan")
    odds_history: Mapped[list["OddsHistory"]] = relationship(back_populates="race", cascade="all, delete-orphan")
    predictions: Mapped[list["Prediction"]] = relationship(back_populates="race", cascade="all, delete-orphan")


class Horse(Base):
    __tablename__ = "horses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)  # HKJC horse code
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_tc: Mapped[str | None] = mapped_column(String(100))  # Traditional Chinese name
    age: Mapped[int | None] = mapped_column(Integer)
    sex: Mapped[str | None] = mapped_column(String(10))  # G (gelding), H (horse), M (mare)
    color: Mapped[str | None] = mapped_column(String(30))
    import_type: Mapped[str | None] = mapped_column(String(10))  # PPG, PP, ISG
    current_rating: Mapped[int | None] = mapped_column(Integer)
    season_stakes: Mapped[float | None] = mapped_column(Float, default=0)
    total_starts: Mapped[int | None] = mapped_column(Integer, default=0)
    total_wins: Mapped[int | None] = mapped_column(Integer, default=0)
    total_places: Mapped[int | None] = mapped_column(Integer, default=0)
    sire: Mapped[str | None] = mapped_column(String(100))
    dam: Mapped[str | None] = mapped_column(String(100))
    dam_sire: Mapped[str | None] = mapped_column(String(100))
    country_of_origin: Mapped[str | None] = mapped_column(String(50))
    owner: Mapped[str | None] = mapped_column(String(200))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(20), default="hkjc")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runners: Mapped[list["Runner"]] = relationship(back_populates="horse")


class Jockey(Base):
    __tablename__ = "jockeys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_tc: Mapped[str | None] = mapped_column(String(100))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(20), default="hkjc")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runners: Mapped[list["Runner"]] = relationship(back_populates="jockey")


class Trainer(Base):
    __tablename__ = "trainers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_tc: Mapped[str | None] = mapped_column(String(100))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(20), default="hkjc")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runners: Mapped[list["Runner"]] = relationship(back_populates="trainer")


class Runner(Base):
    __tablename__ = "runners"
    __table_args__ = (
        UniqueConstraint("race_id", "horse_no", name="uq_runner_in_race"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_id: Mapped[int] = mapped_column(ForeignKey("races.id"), nullable=False, index=True)
    horse_id: Mapped[int | None] = mapped_column(ForeignKey("horses.id"), index=True)
    jockey_id: Mapped[int | None] = mapped_column(ForeignKey("jockeys.id"), index=True)
    trainer_id: Mapped[int | None] = mapped_column(ForeignKey("trainers.id"), index=True)
    horse_no: Mapped[int] = mapped_column(Integer, nullable=False)  # Saddle cloth number
    horse_name: Mapped[str | None] = mapped_column(String(100))
    draw: Mapped[int | None] = mapped_column(Integer)  # Gate position
    actual_weight: Mapped[int | None] = mapped_column(Integer)  # lbs
    declared_weight: Mapped[int | None] = mapped_column(Integer)  # lbs
    handicap_weight: Mapped[int | None] = mapped_column(Integer)
    rating: Mapped[int | None] = mapped_column(Integer)  # HKJC rating at time of race
    rating_change: Mapped[int | None] = mapped_column(Integer)  # +/- from previous
    gear: Mapped[str | None] = mapped_column(String(50))  # Equipment: B(linkers), TT, etc.
    last_6_runs: Mapped[str | None] = mapped_column(String(30))  # e.g. "1/2/3/5/4/1"
    season_stakes: Mapped[float | None] = mapped_column(Float)
    days_since_last_run: Mapped[int | None] = mapped_column(Integer)
    priority: Mapped[int | None] = mapped_column(Integer)  # Ballot priority

    # Results (populated after race)
    finish_position: Mapped[int | None] = mapped_column(Integer)  # 1, 2, 3... or 0 for DNF/scratched
    lbw: Mapped[str | None] = mapped_column(String(20))  # Lengths behind winner, e.g. "3-1/4"
    finish_time: Mapped[str | None] = mapped_column(String(20))
    win_odds: Mapped[float | None] = mapped_column(Float)  # Final win odds
    running_positions: Mapped[str | None] = mapped_column(String(50))  # e.g. "3 3 2 1"
    scratched: Mapped[bool] = mapped_column(Boolean, default=False)

    race: Mapped["Race"] = relationship(back_populates="runners")
    horse: Mapped["Horse | None"] = relationship(back_populates="runners")
    jockey: Mapped["Jockey | None"] = relationship(back_populates="runners")
    trainer: Mapped["Trainer | None"] = relationship(back_populates="runners")
    predictions: Mapped[list["Prediction"]] = relationship(back_populates="runner")


class OddsHistory(Base):
    __tablename__ = "odds_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_id: Mapped[int] = mapped_column(ForeignKey("races.id"), nullable=False, index=True)
    horse_no: Mapped[int] = mapped_column(Integer, nullable=False)
    pool_type: Mapped[str] = mapped_column(String(10), nullable=False)  # WIN, PLA, QIN, QPL, etc.
    odds_value: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    race: Mapped["Race"] = relationship(back_populates="odds_history")


class Dividend(Base):
    __tablename__ = "dividends"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_id: Mapped[int] = mapped_column(ForeignKey("races.id"), nullable=False, index=True)
    pool_type: Mapped[str] = mapped_column(String(10), nullable=False)  # WIN, PLA, QIN, QPL, FCT, TCE, TRI, F4, QTT
    combination: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g. "3", "3,7", "3,7,11"
    payout: Mapped[float] = mapped_column(Float, nullable=False)  # Per $10 unit

    race: Mapped["Race"] = relationship(back_populates="dividends")


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_id: Mapped[int] = mapped_column(ForeignKey("races.id"), nullable=False, index=True)
    runner_id: Mapped[int | None] = mapped_column(ForeignKey("runners.id"), index=True)
    horse_no: Mapped[int] = mapped_column(Integer, nullable=False)
    predicted_win_prob: Mapped[float | None] = mapped_column(Float)
    predicted_place_prob: Mapped[float | None] = mapped_column(Float)
    predicted_rank: Mapped[int | None] = mapped_column(Integer)
    is_value_bet: Mapped[bool] = mapped_column(Boolean, default=False)
    bet_type: Mapped[str | None] = mapped_column(String(10))  # WIN, PLA, QIN, etc.
    bet_combination: Mapped[str | None] = mapped_column(String(50))  # For exotic bets
    bet_amount: Mapped[float | None] = mapped_column(Float)
    model_version: Mapped[str | None] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    race: Mapped["Race"] = relationship(back_populates="predictions")
    runner: Mapped["Runner | None"] = relationship(back_populates="predictions")
    bet_result: Mapped["BetResult | None"] = relationship(back_populates="prediction", uselist=False)


class BetResult(Base):
    __tablename__ = "bet_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prediction_id: Mapped[int] = mapped_column(ForeignKey("predictions.id"), unique=True, nullable=False)
    actual_position: Mapped[int | None] = mapped_column(Integer)
    actual_dividend: Mapped[float | None] = mapped_column(Float)
    profit_loss: Mapped[float] = mapped_column(Float, nullable=False)  # Net P&L for this bet
    settled: Mapped[bool] = mapped_column(Boolean, default=False)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime)

    prediction: Mapped["Prediction"] = relationship(back_populates="bet_result")


class ModelMetadata(Base):
    __tablename__ = "model_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    version: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    model_type: Mapped[str] = mapped_column(String(20), nullable=False)  # lightgbm, xgboost
    target: Mapped[str] = mapped_column(String(20), nullable=False)  # win, place
    trained_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    training_races_count: Mapped[int | None] = mapped_column(Integer)
    training_date_range: Mapped[str | None] = mapped_column(String(50))  # "2021-09-01 to 2025-12-31"
    validation_metric: Mapped[float | None] = mapped_column(Float)  # e.g. ROC-AUC
    hyperparams: Mapped[dict | None] = mapped_column(JSON)
    feature_importance: Mapped[dict | None] = mapped_column(JSON)
    model_path: Mapped[str | None] = mapped_column(String(300))
    notes: Mapped[str | None] = mapped_column(Text)
