from deltacat.exceptions import DeltaCatError


class EntityNotFound(DeltaCatError):
    error_name = "EntityNotFound"
    is_retryable = False
