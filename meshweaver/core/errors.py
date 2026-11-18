
"""Shared exception types used across MeshWeaver."""


class MeshWeaverError(Exception):
    """Base exception for all MeshWeaver errors."""


class ConfigurationError(MeshWeaverError):
    """Raised when application configuration is invalid."""


class ProtocolError(MeshWeaverError):
    """Raised when a protocol message is invalid."""


class StorageError(MeshWeaverError):
    """Raised when a storage operation fails."""


class TaskError(MeshWeaverError):
    """Raised when task processing fails."""
