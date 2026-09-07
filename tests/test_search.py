from __future__ import annotations

import asyncio
from typing import Any

import pytest

from fluxer import Channel, Client, Guild, SearchIndexing, SearchResult
from fluxer.fluxer_models import parse_search_response
from fluxer.http import HTTPClient


def test_search_messages_builds_complete_payload() -> None:
    async def run() -> None:
        http: Any = HTTPClient("token", is_bot=False)
        calls: list[tuple[str, str, dict[str, Any]]] = []

        async def request(route, **kwargs):
            calls.append((route.method, route.path, kwargs))
            return {
                "messages": [],
                "channels": [],
                "total": 0,
                "hits_per_page": 25,
                "page": 1,
            }

        http.request = request
        await http.search_messages(
            scope="all",
            context_channel_id=10,
            context_guild_id="20",
            channel_ids=[10, "11"],
            channel_id=[12],
            hits_per_page=25,
            page=1,
            cursor=["opaque"],
            min_id=100,
            max_id="200",
            content="hello",
            contents=["one", "two"],
            exact_phrases=["exact phrase"],
            exclude_channel_id=[13],
            author_id=[30],
            exclude_author_id=[31],
            author_type=["user", "bot"],
            exclude_author_type=["webhook"],
            mentions=[40],
            exclude_mentions=[41],
            mention_everyone=False,
            pinned=False,
            has=["image", "link"],
            exclude_has=["video"],
            embed_type=["article"],
            exclude_embed_type=["image"],
            embed_provider=["Example"],
            exclude_embed_provider=["Blocked"],
            link_hostname=["example.com"],
            exclude_link_hostname=["blocked.invalid"],
            attachment_filename=["report.pdf"],
            exclude_attachment_filename=["draft.txt"],
            attachment_extension=["pdf"],
            exclude_attachment_extension=["exe"],
            sort_by="timestamp",
            sort_order="desc",
            include_nsfw=False,
        )

        assert calls == [
            (
                "POST",
                "/search/messages",
                {
                    "json": {
                        "scope": "all",
                        "context_channel_id": "10",
                        "context_guild_id": "20",
                        "channel_ids": ["10", "11"],
                        "channel_id": ["12"],
                        "hits_per_page": 25,
                        "page": 1,
                        "cursor": ["opaque"],
                        "min_id": "100",
                        "max_id": "200",
                        "content": "hello",
                        "contents": ["one", "two"],
                        "exact_phrases": ["exact phrase"],
                        "exclude_channel_id": ["13"],
                        "author_id": ["30"],
                        "exclude_author_id": ["31"],
                        "author_type": ["user", "bot"],
                        "exclude_author_type": ["webhook"],
                        "mentions": ["40"],
                        "exclude_mentions": ["41"],
                        "mention_everyone": False,
                        "pinned": False,
                        "has": ["image", "link"],
                        "exclude_has": ["video"],
                        "embed_type": ["article"],
                        "exclude_embed_type": ["image"],
                        "embed_provider": ["Example"],
                        "exclude_embed_provider": ["Blocked"],
                        "link_hostname": ["example.com"],
                        "exclude_link_hostname": ["blocked.invalid"],
                        "attachment_filename": ["report.pdf"],
                        "exclude_attachment_filename": ["draft.txt"],
                        "attachment_extension": ["pdf"],
                        "exclude_attachment_extension": ["exe"],
                        "sort_by": "timestamp",
                        "sort_order": "desc",
                        "include_nsfw": False,
                    }
                },
            )
        ]

    asyncio.run(run())


def test_search_messages_omits_unspecified_values() -> None:
    async def run() -> None:
        http: Any = HTTPClient("token")
        payload: dict[str, Any] = {}

        async def request(route, **kwargs):
            payload.update(kwargs["json"])
            return {}

        http.request = request
        await http.search_messages(pinned=False)
        assert payload == {"pinned": False}

    asyncio.run(run())


def test_search_messages_requested_global_search_shape() -> None:
    async def run() -> None:
        http: Any = HTTPClient("token", is_bot=False)
        payload: dict[str, Any] = {}

        async def request(route, **kwargs):
            payload.update(kwargs["json"])
            return {}

        http.request = request
        await http.search_messages(
            hits_per_page=25,
            page=1,
            sort_by="timestamp",
            sort_order="desc",
            scope="all",
        )
        assert payload == {
            "hits_per_page": 25,
            "page": 1,
            "sort_by": "timestamp",
            "sort_order": "desc",
            "scope": "all",
        }

    asyncio.run(run())


def test_search_response_models_and_channel_linking() -> None:
    data = {
        "messages": [
            {
                "id": "101",
                "channel_id": "10",
                "content": "found",
                "author": {"id": "42", "username": "tester"},
                "timestamp": "2026-01-01T00:00:00+00:00",
                "message_reference": {"message_id": "100", "channel_id": "10"},
            }
        ],
        "channels": [{"id": "10", "type": 0, "name": "general", "guild_id": "20"}],
        "total": 1,
        "hits_per_page": 25,
        "page": 2,
        "cursor": ["opaque", "values"],
    }

    result = parse_search_response(data)
    assert isinstance(result, SearchResult)
    assert result.total == 1
    assert result.page == 2
    assert result.cursor == ["opaque", "values"]
    assert result.next_cursor == result.cursor
    assert result.messages[0].referenced_message is None
    assert result.messages[0].channel is result.channels[0]

    indexing = parse_search_response({"indexing": True})
    assert isinstance(indexing, SearchIndexing)
    assert indexing.indexing is True


def test_bot_search_helpers_inject_current_context() -> None:
    async def run() -> None:
        http: Any = HTTPClient("token")
        payloads: list[dict[str, Any]] = []

        async def request(route, **kwargs):
            payloads.append(kwargs["json"])
            return {
                "messages": [],
                "channels": [],
                "total": 0,
                "hits_per_page": 5,
                "page": 1,
            }

        http.request = request
        client = Client()
        client._http = http
        guild = Guild(id=20, _http=http)
        channel = Channel(id=10, type=0, guild_id=20, _http=http)

        assert isinstance(
            await client.search_messages(context_channel_id=10, content="client"),
            SearchResult,
        )
        assert isinstance(await guild.search_messages(content="guild"), SearchResult)
        assert isinstance(
            await channel.search_messages(content="channel"), SearchResult
        )

        assert payloads[0]["scope"] == "current"
        assert payloads[0]["context_channel_id"] == "10"
        assert payloads[1]["scope"] == "current"
        assert payloads[1]["context_guild_id"] == "20"
        assert payloads[2]["scope"] == "current"
        assert payloads[2]["context_channel_id"] == "10"

        with pytest.raises(ValueError, match="context_channel_id or context_guild_id"):
            await client.search_messages(content="missing context")

    asyncio.run(run())
