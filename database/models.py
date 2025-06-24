from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

Base = declarative_base()

class DataQualityStat(Base):
    __tablename__ = "data_quality_stats"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String, nullable=False)
    total_rows = Column(Integer, nullable=False)
    valid_rows = Column(Integer, nullable=False)
    invalid_rows = Column(Integer, nullable=False)
    error_counts = Column(Text, nullable=True)  # Great Expectations errors (JSON)
    is_critical = Column(Boolean, default=False, nullable=False)
    error_type = Column(String, nullable=True)  # e.g., 'spicejet_blank_price'
    error_count = Column(Integer, nullable=True)  # Count for this error type
    severity = Column(String, nullable=True)  # e.g., 'high', 'medium', 'low'
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import Column, Integer, String, JSON, DateTime
# from datetime import datetime, timezone

# Base = declarative_base()

# class DataQualityStat(Base):
#     __tablename__ = "data_quality_stats"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     filename = Column(String, nullable=False)
#     total_rows = Column(Integer, nullable=False)
#     valid_rows = Column(Integer, nullable=False)
#     invalid_rows = Column(Integer, nullable=False)
#     error_counts = Column(JSON, nullable=True) 
#     timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))