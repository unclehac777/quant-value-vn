"""
Supabase client initialisation.

Provides a lazy-initialised Supabase client singleton.
Requires SUPABASE_URL and SUPABASE_KEY environment variables.
"""

from typing import Optional
from supabase import create_client, Client

from quant_value_vn.config import SUPABASE_URL, SUPABASE_KEY

_client: Optional[Client] = None


def get_client() -> Client:
    """Lazy-init Supabase client."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "Set SUPABASE_URL and SUPABASE_KEY in .env or environment"
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client
