"""
Pytest fixtures for horse racing prediction system.
"""

import pytest
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base, Race, Runner, Horse, Jockey, Trainer, Dividend


@pytest.fixture
def db_session():
    """Create an in-memory SQLite session for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_race(db_session):
    """Create a sample race with runners for testing."""
    race = Race(
        race_date=date(2025, 12, 14),
        racecourse="ST",
        race_no=1,
        race_class="Class 3",
        distance=1200,
        track_type="Turf",
        course_variant="A",
        going="Good",
        prize=1200000,
        field_size=14,
        source="hkjc",
        season="2025/26",
    )
    db_session.add(race)

    # Add a horse
    horse = Horse(code="HK_2025_A001", name="Golden Star", name_tc="金星", age=5, sex="G",
                  current_rating=78, total_starts=20, total_wins=3, total_places=6)
    db_session.add(horse)

    jockey = Jockey(code="JM", name="J. Moreira")
    trainer = Trainer(code="CS", name="C. Size")
    db_session.add_all([jockey, trainer])
    db_session.flush()

    # Add runners
    for i in range(1, 15):
        runner = Runner(
            race_id=race.id,
            horse_id=horse.id if i == 3 else None,
            jockey_id=jockey.id if i == 3 else None,
            trainer_id=trainer.id if i == 3 else None,
            horse_no=i,
            horse_name=f"Horse {i}" if i != 3 else "Golden Star",
            draw=i,
            actual_weight=126,
            rating=60 + i * 2,
            win_odds=float(3 + i * 2),
            finish_position=i,
            last_6_runs="1/2/3/5/4/2" if i == 3 else f"{i}/{i+1}/{i-1}/5/7/8",
        )
        db_session.add(runner)

    # Add dividends
    db_session.add(Dividend(race_id=race.id, pool_type="WIN", combination="3", payout=55.0))
    db_session.add(Dividend(race_id=race.id, pool_type="PLA", combination="3", payout=18.0))
    db_session.add(Dividend(race_id=race.id, pool_type="QIN", combination="3,7", payout=120.0))

    db_session.commit()
    return race
