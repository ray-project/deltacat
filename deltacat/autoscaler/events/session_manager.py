import uuid
from typing import Optional


class SessionManager:
    def __init__(self,
                 session_id: Optional[str] = None,
                 parent_session_id: Optional[str] = None):
        """

        Args:
            session_id: Overrideable Session ID for this Ray app instance. If not provided, a Session ID is generated.
            parent_session_id: Overrideable Parent Session ID for this Ray app instance.
        """
        if not session_id:
            session_id = str(uuid.uuid4())

        self._session_id = session_id
        self._parent_session_id = parent_session_id

    @property
    def session_id(self):
        return self._session_id

    @property
    def parent_session_id(self):
        return self._parent_session_id
