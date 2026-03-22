"""Data models package for type-safe data structures."""
from .candidate import Candidate
from .vote import Vote
from .voter import Address, Voter

__all__ = ["Voter", "Address", "Candidate", "Vote"]
