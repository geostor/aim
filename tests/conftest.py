"""Tests run against a separate database, so they never wipe a running dev server."""

import asyncio
import os

import asyncpg
import pytest

ADMIN_URL = os.environ.get("TEST_ADMIN_DATABASE_URL", "postgresql://aim:aim@127.0.0.1:5434/aim")
TEST_DATABASE_URL = ADMIN_URL.rsplit("/", 1)[0] + "/aim_test"


async def _create_test_database():
    db = await asyncpg.connect(ADMIN_URL)
    try:
        if not await db.fetchval("SELECT 1 FROM pg_database WHERE datname = 'aim_test'"):
            await db.execute("CREATE DATABASE aim_test")
    finally:
        await db.close()


@pytest.fixture(scope="session")
def test_database():
    asyncio.run(_create_test_database())
    return TEST_DATABASE_URL
