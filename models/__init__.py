"""Data models package for type-safe data structures."""
from .voter import Voter, Address
from .candidate import Candidate
from .vote import Vote

__all__ = ['Voter', 'Address', 'Candidate', 'Vote']