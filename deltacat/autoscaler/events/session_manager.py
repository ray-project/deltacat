import uuid
from typing import Optional


PARENT_SESSION_ID_KEY = "rayParentSessionId"
SESSION_ID_KEY = "raySessionId"


class SessionManager:
    def __init__(self,
                 session_id: Optional[str] = None):
        """Manages Ray sessions.

        Args:
            session_id: Overrideable Session ID for this Ray app instance.
             If not provided, a Session ID is newly generated.
        """
        if not session_id:
            session_id = str(uuid.uuid4())

        self._session_id = session_id

    @property
    def session_id(self):
        return self._session_id
