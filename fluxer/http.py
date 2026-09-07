from __future__ import annotations

import asyncio
import logging
from typing import Any
from .models.embed import Embed

import aiohttp
import json as json_mod

from .errors import http_exception_from_status
from .fluxer_models import (
    SearchAuthorType,
    SearchContentType,
    SearchEmbedType,
    SearchScope,
    SearchSortBy,
    SearchSortOrder,
)

log = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.fluxer.app/v1"


def _payload_value(value: Any) -> Any:
    return value.to_dict() if hasattr(value, "to_dict") else value


def _get_user_agent() -> str:
    """Get the user agent string with the current version."""
    from . import __version__

    return f"fluxer.py/{__version__} (https://github.com/akarealemil/fluxer.py)"


class Route:
    """Represents an API route. Used for rate limit bucketing.

    Usage:
        route = Route("GET", "/channels/{channel_id}/messages", channel_id="123", base_url="https://api.fluxer.app/v1")
        # route.url = "https://api.fluxer.app/v1/channels/123/messages"
        # route.bucket = "GET /channels/{channel_id}/messages"
    """

    def __init__(
        self, method: str, path: str, base_url: str = DEFAULT_API_URL, **params: Any
    ) -> None:
        self.method = method
        self.path = path
        self.base_url = base_url
        # Convert all parameters to strings for URL formatting (handles int IDs)
        self.params = {k: str(v) for k, v in params.items()}
        self.url = self.base_url + path.format(**self.params)

        # Rate limit bucket key: method + path template + major params
        # Major params (channel_id, guild_id) get their own buckets
        self.bucket = f"{method} {path}"
        for key in ("channel_id", "guild_id", "webhook_id"):
            if key in self.params:
                self.bucket += f":{self.params[key]}"


class RateLimiter:
    """Per-route rate limit handler using Fluxer's response headers.

    Fluxer returns rate limit info via HTTP headers:
        X-RateLimit-Limit: max requests in window
        X-RateLimit-Remaining: requests left
        X-RateLimit-Reset: Unix timestamp when the limit resets
        X-RateLimit-Reset-After: seconds until reset
        X-RateLimit-Bucket: opaque bucket identifier
    """

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._reset_times: dict[str, float] = {}
        self._global_lock = asyncio.Event()
        self._global_lock.set()  # Not locked initially

    def _get_lock(self, bucket: str) -> asyncio.Lock:
        if bucket not in self._locks:
            self._locks[bucket] = asyncio.Lock()
        return self._locks[bucket]

    async def acquire(self, bucket: str) -> None:
        """Wait if this bucket or global rate limit is active."""
        # Wait for global rate limit to clear
        await self._global_lock.wait()

        lock = self._get_lock(bucket)
        await lock.acquire()

        # Check if we need to wait for this bucket
        reset_at = self._reset_times.get(bucket)
        if reset_at is not None:
            now = asyncio.get_event_loop().time()
            if now < reset_at:
                delay = reset_at - now
                log.debug("Rate limited on bucket %s, waiting %.2fs", bucket, delay)
                await asyncio.sleep(delay)

    def release(self, bucket: str, headers: dict[str, str]) -> None:
        """Update rate limit state from response headers and release the lock."""
        remaining = headers.get("X-RateLimit-Remaining")
        reset_after = headers.get("X-RateLimit-Reset-After")

        if remaining is not None and int(remaining) == 0 and reset_after is not None:
            delay = float(reset_after)
            self._reset_times[bucket] = asyncio.get_event_loop().time() + delay
            log.debug("Bucket %s exhausted, reset in %.2fs", bucket, delay)
        else:
            self._reset_times.pop(bucket, None)

        lock = self._get_lock(bucket)
        if lock.locked():
            lock.release()

    def set_global(self, retry_after: float) -> None:
        """Activate a global rate limit."""
        self._global_lock.clear()
        log.warning("Global rate limit hit, pausing for %.2fs", retry_after)

        async def _unlock() -> None:
            await asyncio.sleep(retry_after)
            self._global_lock.set()

        asyncio.ensure_future(_unlock())


class HTTPClient:
    """Async HTTP client for the Fluxer REST API.

    Usage:
        async with HTTPClient(token) as http:
            data = await http.request(Route("GET", "/users/@me"))

    Args:
        token: Bot or user token
        is_bot: Whether this is a bot token (adds "Bot" prefix to auth header)
        api_url: Base URL for the API (default: https://api.fluxer.app/v1)
                 Use this to connect to self-hosted Fluxer instances
    """

    def __init__(
        self,
        token: str,
        *,
        is_bot: bool = True,
        api_url: str = DEFAULT_API_URL,
        max_retries=4,
        retry_forever=False,
    ) -> None:
        self.token = token
        self.is_bot = is_bot
        self.api_url = api_url.rstrip("/")  # Remove trailing slash if present
        self._session: aiohttp.ClientSession | None = None
        self._rate_limiter = RateLimiter()
        self.max_retries = max_retries
        self.retry_forever = retry_forever

    def _route(self, method: str, path: str, **params: Any) -> Route:
        """Create a Route with this client's API URL."""
        return Route(method, path, base_url=self.api_url, **params)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Use "Bot" prefix for bot tokens, plain token for user tokens
            auth_header = f"Bot {self.token}" if self.is_bot else self.token
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": auth_header,
                    "User-Agent": _get_user_agent(),
                }
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> HTTPClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def request(
        self,
        route: Route,
        *,
        json: Any = None,
        data: aiohttp.FormData | None = None,
        params: dict[str, Any] | None = None,
        reason: str | None = None,
        max_retries: int | None = None,
        retry_forever: bool | None = None,
    ) -> Any:
        """Make an authenticated request to the Fluxer API.

        Handles rate limiting, retries on 429/5xx, connection errors, and error mapping.

        Returns:
            Parsed JSON response, or None for 204 No Content.
        """
        session = await self._ensure_session()

        headers: dict[str, str] = {}
        if reason:
            headers["X-Audit-Log-Reason"] = reason
        if json is not None:
            headers["Content-Type"] = "application/json"

        if max_retries is None:
            max_retries = self.max_retries
        if retry_forever is None:
            retry_forever = self.retry_forever

        attempt = 0
        while True:
            await self._rate_limiter.acquire(route.bucket)

            try:
                async with session.request(
                    route.method,
                    route.url,
                    json=json,
                    data=data,
                    params=params,
                    headers=headers,
                ) as resp:
                    resp_headers = {k: v for k, v in resp.headers.items()}
                    self._rate_limiter.release(route.bucket, resp_headers)

                    # Success
                    if 200 <= resp.status < 300:
                        if resp.status == 204:
                            return None
                        return await resp.json()

                    # Rate limited
                    if resp.status == 429:
                        body = await resp.json()
                        retry_after = body.get("retry_after", 1.0)
                        is_global = body.get("global", False)

                        if is_global:
                            self._rate_limiter.set_global(retry_after)
                        else:
                            log.warning(
                                "Rate limited on %s, retry in %.2fs (attempt %d)",
                                route.url,
                                retry_after,
                                attempt + 1,
                            )
                            await asyncio.sleep(retry_after)
                        attempt += 1
                        if not retry_forever and attempt > max_retries:
                            raise RuntimeError(
                                f"Failed after {attempt} attempts: {route.method} {route.url}"
                            )
                        continue

                    # Server error — retry
                    if resp.status >= 500:
                        log.warning(
                            "Server error %d on %s, retrying (attempt %d)",
                            resp.status,
                            route.url,
                            attempt + 1,
                        )
                        await asyncio.sleep(1 + attempt)
                        attempt += 1
                        if not retry_forever and attempt > max_retries:
                            raise RuntimeError(
                                f"Failed after {attempt} attempts: {route.method} {route.url}"
                            )
                        continue

                    # Client error — raise
                    body = await resp.json()
                    raise http_exception_from_status(
                        status=resp.status,
                        code=body.get("code", "UNKNOWN"),
                        message=body.get("message", "Unknown error"),
                        errors=body.get("errors"),
                        retry_after=body.get("retry_after", 0.0),
                    )

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self._rate_limiter.release(route.bucket, {})
                log.warning(
                    "Connection error: %s, retrying (attempt %d)", exc, attempt + 1
                )
                attempt += 1
                if not retry_forever and attempt > max_retries:
                    raise
                await asyncio.sleep(1 + attempt)
                continue

    # =========================================================================
    # Convenience methods for common endpoints
    # =========================================================================

    # -- Gateway --
    async def get_gateway(self) -> dict[str, Any]:
        """GET /gateway/bot — get the WebSocket URL.

        Note: Fluxer's /gateway endpoint returns 404 for bots.
        This method uses /gateway/bot instead (bot tokens only).
        """
        return await self.get_gateway_bot()

    async def get_gateway_bot(self) -> dict[str, Any]:
        """GET /gateway/bot — get gateway URL + sharding info."""
        return await self.request(self._route("GET", "/gateway/bot"))

    # -- Users --
    async def get_current_user(self) -> dict[str, Any]:
        """GET /users/@me"""
        return await self.request(self._route("GET", "/users/@me"))

    async def get_user(self, user_id: int | str) -> dict[str, Any]:
        """GET /users/{user_id}"""
        return await self.request(
            self._route("GET", "/users/{user_id}", user_id=user_id)
        )

    async def get_user_profile(
        self, user_id: int | str, *, guild_id: int | str | None = None
    ) -> dict[str, Any]:
        """GET /users/{user_id}/profile — Get a user's full profile.

        This returns additional profile information like bio, pronouns, banner, etc.
        that is not included in the basic user object.

        Args:
            user_id: The user ID to fetch
            guild_id: Optional guild ID for guild-specific profile data

        Returns:
            Profile object containing:
            - user: Basic user object
            - user_profile: Profile data (bio, pronouns, banner, etc.)
            - premium_type, premium_since, premium_lifetime_sequence
        """
        route = self._route("GET", "/users/{user_id}/profile", user_id=user_id)
        params = {"guild_id": str(guild_id)} if guild_id else None
        return await self.request(route, params=params)

    async def create_dm(self, user_id: int | str) -> dict[str, Any]:
        """POST /users/@me/channels — Open a DM channel with a user.
        Args:
            user_id: The ID of the user to open a DM with.

        Returns:
            Channel object for the DM channel.
        """
        return await self.request(
            self._route("POST", "/users/@me/channels"),
            json={"recipient_id": str(user_id)},
        )

    async def get_current_user_guilds(self) -> list[dict[str, Any]]:
        """GET /users/@me/guilds - get guilds the current user is in"""
        return await self.request(self._route("GET", "/users/@me/guilds"))

    # -- Channels --
    async def get_channel(self, channel_id: int | str) -> dict[str, Any]:
        """GET /channels/{channel_id}"""
        return await self.request(
            self._route("GET", "/channels/{channel_id}", channel_id=channel_id)
        )

    async def trigger_typing(self, channel_id: int | str) -> None:
        return await self.request(
            self._route("POST", "/channels/{channel_id}/typing", channel_id=channel_id)
        )

    async def get_channel_invites(self, channel_id: int | str) -> list[dict[str, Any]]:
        """GET /channels/{channel_id}/invites"""
        return await self.request(
            self._route("GET", "/channels/{channel_id}/invites", channel_id=channel_id)
        )

    async def create_channel_invite(
        self, channel_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """POST /channels/{channel_id}/invites"""
        return await self.request(
            self._route(
                "POST", "/channels/{channel_id}/invites", channel_id=channel_id
            ),
            json=payload,
        )

    # -- Messages --
    async def send_message(
        self,
        channel_id: int | str,
        *,
        content: str | None = None,
        embed: Any | None = None,  # NEW (single embed support)
        embeds: list[Any] | None = None,
        files: list[Any] | None = None,
        message_reference: dict[str, Any] | None = None,
        allowed_mentions: Any | None = None,
        flags: int | None = None,
        nonce: str | int | None = None,
        favorite_meme_id: int | str | None = None,
        sticker_ids: list[int | str] | None = None,
        tts: bool | None = None,
    ) -> dict[str, Any]:
        """POST /channels/{channel_id}/messages

        Args:
            channel_id: The channel to send the message to
            content: The message content
            embed: Single embed object (optional)
            embeds: List of embed objects (Embed or dict)
            files: List of file objects to attach
            message_reference: Reference to another message for replies
                Example: {"message_id": "123456789", "channel_id": "987654321"}
            allowed_mentions: Controls which mentions trigger notifications
        """
        route = self._route(
            "POST",
            "/channels/{channel_id}/messages",
            channel_id=channel_id,
        )

        payload: dict[str, Any] = {}

        if content is not None:
            payload["content"] = content

        # --- Normalize embed(s) ---

        # Support single embed param
        if embed is not None:
            embeds = [embed]

        # Normalize all embeds
        if embeds is not None:
            normalized = []
            for e in embeds:
                if isinstance(e, Embed):
                    normalized.append(e.to_dict())
                else:
                    normalized.append(e)
            payload["embeds"] = normalized

        if message_reference is not None:
            payload["message_reference"] = _payload_value(message_reference)
        if allowed_mentions is not None:
            payload["allowed_mentions"] = _payload_value(allowed_mentions)
        if flags is not None:
            payload["flags"] = flags
        if nonce is not None:
            payload["nonce"] = nonce
        if favorite_meme_id is not None:
            payload["favorite_meme_id"] = str(favorite_meme_id)
        if sticker_ids is not None:
            payload["sticker_ids"] = [str(sticker_id) for sticker_id in sticker_ids]
        if tts is not None:
            payload["tts"] = tts

        # --- File handling ---
        if files:
            form = aiohttp.FormData()

            payload["attachments"] = [
                {"id": i, "filename": file["filename"]} for i, file in enumerate(files)
            ]

            form.add_field(
                "payload_json",
                json_mod.dumps(payload),
                content_type="application/json",
            )

            for i, file in enumerate(files):
                form.add_field(
                    f"files[{i}]",
                    file["data"],
                    filename=file["filename"],
                )

            return await self.request(route, data=form)

        return await self.request(route, json=payload)

    async def get_message(
        self,
        channel_id: int | str,
        message_id: int | str,
    ) -> dict[str, Any]:
        """GET /channels/{channel_id}/messages/{message_id} - Fetch a single message"""
        route = self._route(
            "GET",
            "/channels/{channel_id}/messages/{message_id}",
            channel_id=channel_id,
            message_id=message_id,
        )
        return await self.request(route)

    async def get_messages(
        self,
        channel_id: int | str,
        *,
        limit: int = 50,
        before: int | str | None = None,
        after: int | str | None = None,
        around: int | str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /channels/{channel_id}/messages"""
        params: dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        if around:
            params["around"] = around

        route = self._route(
            "GET", "/channels/{channel_id}/messages", channel_id=channel_id
        )
        return await self.request(route, params=params)

    async def edit_message(
        self,
        channel_id: int | str,
        message_id: int | str,
        *,
        content: str | None = None,
        embeds: list[dict[str, Any]] | None = None,
        allowed_mentions: Any | None = None,
        flags: int | None = None,
        attachments: list[dict[str, Any]] | None = None,
        message_snapshots: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """PATCH /channels/{channel_id}/messages/{message_id}"""
        route = self._route(
            "PATCH",
            "/channels/{channel_id}/messages/{message_id}",
            channel_id=channel_id,
            message_id=message_id,
        )
        payload: dict[str, Any] = {}
        if content is not None:
            payload["content"] = content
        if embeds is not None:
            payload["embeds"] = embeds
        if allowed_mentions is not None:
            payload["allowed_mentions"] = _payload_value(allowed_mentions)
        if flags is not None:
            payload["flags"] = flags
        if attachments is not None:
            payload["attachments"] = attachments
        if message_snapshots is not None:
            payload["message_snapshots"] = message_snapshots
        return await self.request(route, json=payload)

    async def delete_message(
        self, channel_id: int | str, message_id: int | str
    ) -> None:
        """DELETE /channels/{channel_id}/messages/{message_id}"""
        route = self._route(
            "DELETE",
            "/channels/{channel_id}/messages/{message_id}",
            channel_id=channel_id,
            message_id=message_id,
        )
        await self.request(route)

    async def delete_messages(
        self, channel_id: int | str, message_ids: list[int | str]
    ) -> None:
        """POST /channels/{channel_id}/messages/bulk-delete"""
        route = self._route(
            "POST",
            "/channels/{channel_id}/messages/bulk-delete",
            channel_id=channel_id,
        )
        payload = {"message_ids": [str(mid) for mid in message_ids]}
        await self.request(route, json=payload)

    async def search_messages(
        self,
        *,
        scope: SearchScope | None = None,
        context_channel_id: int | str | None = None,
        context_guild_id: int | str | None = None,
        channel_ids: list[int | str] | None = None,
        channel_id: list[int | str] | None = None,
        hits_per_page: int | None = None,
        page: int | None = None,
        cursor: list[str] | None = None,
        min_id: int | str | None = None,
        max_id: int | str | None = None,
        content: str | None = None,
        contents: list[str] | None = None,
        exact_phrases: list[str] | None = None,
        exclude_channel_id: list[int | str] | None = None,
        author_id: list[int | str] | None = None,
        exclude_author_id: list[int | str] | None = None,
        author_type: list[SearchAuthorType] | None = None,
        exclude_author_type: list[SearchAuthorType] | None = None,
        mentions: list[int | str] | None = None,
        exclude_mentions: list[int | str] | None = None,
        mention_everyone: bool | None = None,
        pinned: bool | None = None,
        has: list[SearchContentType] | None = None,
        exclude_has: list[SearchContentType] | None = None,
        embed_type: list[SearchEmbedType] | None = None,
        exclude_embed_type: list[SearchEmbedType] | None = None,
        embed_provider: list[str] | None = None,
        exclude_embed_provider: list[str] | None = None,
        link_hostname: list[str] | None = None,
        exclude_link_hostname: list[str] | None = None,
        attachment_filename: list[str] | None = None,
        exclude_attachment_filename: list[str] | None = None,
        attachment_extension: list[str] | None = None,
        exclude_attachment_extension: list[str] | None = None,
        sort_by: SearchSortBy | None = None,
        sort_order: SearchSortOrder | None = None,
        include_nsfw: bool | None = None,
    ) -> dict[str, Any]:
        """POST /search/messages - Search indexed Fluxer messages.

        User-token clients may use every documented scope. Bot tokens are restricted
        by Fluxer to ``current`` and must provide a guild or channel context. Pagination
        uses ``page``; the API accepts but does not honour ``cursor``.
        """
        payload: dict[str, Any] = {}
        values = {
            "scope": scope,
            "hits_per_page": hits_per_page,
            "page": page,
            "cursor": cursor,
            "content": content,
            "contents": contents,
            "exact_phrases": exact_phrases,
            "author_type": author_type,
            "exclude_author_type": exclude_author_type,
            "mention_everyone": mention_everyone,
            "pinned": pinned,
            "has": has,
            "exclude_has": exclude_has,
            "embed_type": embed_type,
            "exclude_embed_type": exclude_embed_type,
            "embed_provider": embed_provider,
            "exclude_embed_provider": exclude_embed_provider,
            "link_hostname": link_hostname,
            "exclude_link_hostname": exclude_link_hostname,
            "attachment_filename": attachment_filename,
            "exclude_attachment_filename": exclude_attachment_filename,
            "attachment_extension": attachment_extension,
            "exclude_attachment_extension": exclude_attachment_extension,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "include_nsfw": include_nsfw,
        }
        payload.update(
            {key: value for key, value in values.items() if value is not None}
        )

        scalar_ids = {
            "context_channel_id": context_channel_id,
            "context_guild_id": context_guild_id,
            "min_id": min_id,
            "max_id": max_id,
        }
        payload.update(
            {key: str(value) for key, value in scalar_ids.items() if value is not None}
        )

        list_ids = {
            "channel_ids": channel_ids,
            "channel_id": channel_id,
            "exclude_channel_id": exclude_channel_id,
            "author_id": author_id,
            "exclude_author_id": exclude_author_id,
            "mentions": mentions,
            "exclude_mentions": exclude_mentions,
        }
        payload.update(
            {
                key: [str(item) for item in value]
                for key, value in list_ids.items()
                if value is not None
            }
        )
        return await self.request(self._route("POST", "/search/messages"), json=payload)

    # -- Pinned Messages --
    async def get_pinned_messages(
        self,
        channel_id: int | str,
        *,
        limit: int | None = None,
        before: int | str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /channels/{channel_id}/pins - Get all pinned messages in a channel.

        Args:
            channel_id: The channel ID to get pinned messages from.

        Returns:
            List of pinned message objects.
        """
        route = self._route("GET", "/channels/{channel_id}/pins", channel_id=channel_id)
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if before is not None:
            params["before"] = before
        return await self.request(route, params=params or None)

    async def pin_message(self, channel_id: int | str, message_id: int | str) -> None:
        """PUT /channels/{channel_id}/pins/{message_id} - Pin a message.

        Args:
            channel_id: The channel ID containing the message.
            message_id: The message ID to pin.

        Returns:
            None (204 No Content)
        """
        route = self._route(
            "PUT",
            "/channels/{channel_id}/pins/{message_id}",
            channel_id=channel_id,
            message_id=message_id,
        )
        await self.request(route)

    async def unpin_message(self, channel_id: int | str, message_id: int | str) -> None:
        """DELETE /channels/{channel_id}/pins/{message_id} - Unpin a message.

        Args:
            channel_id: The channel ID containing the message.
            message_id: The message ID to unpin.

        Returns:
            None (204 No Content)
        """
        route = self._route(
            "DELETE",
            "/channels/{channel_id}/pins/{message_id}",
            channel_id=channel_id,
            message_id=message_id,
        )
        await self.request(route)

    async def ack_message(self, channel_id: int | str, message_id: int | str) -> None:
        """POST /channels/{channel_id}/messages/{message_id}/ack"""
        route = self._route(
            "POST",
            "/channels/{channel_id}/messages/{message_id}/ack",
            channel_id=channel_id,
            message_id=message_id,
        )
        await self.request(route, json={})

    async def ack_pins(self, channel_id: int | str) -> None:
        """POST /channels/{channel_id}/pins/ack"""
        route = self._route(
            "POST", "/channels/{channel_id}/pins/ack", channel_id=channel_id
        )
        await self.request(route, json={})

    async def acknowledge_message(
        self, channel_id: int | str, message_id: int | str
    ) -> None:
        """Alias for acknowledging a Fluxer message."""
        await self.ack_message(channel_id, message_id)

    async def acknowledge_pins(self, channel_id: int | str) -> None:
        """Alias for acknowledging a channel's pin state."""
        await self.ack_pins(channel_id)

    # -- Guilds --
    async def get_guild(self, guild_id: int | str) -> dict[str, Any]:
        """GET /guilds/{guild_id}"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}", guild_id=guild_id)
        )

    async def get_guild_channels(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/channels"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/channels", guild_id=guild_id)
        )

    async def get_guild_invites(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/invites"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/invites", guild_id=guild_id)
        )

    async def get_guild_audit_logs(
        self, guild_id: int | str, **params: Any
    ) -> dict[str, Any]:
        """GET /guilds/{guild_id}/audit-logs"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/audit-logs", guild_id=guild_id),
            params=params or None,
        )

    async def get_guild_member(
        self, guild_id: int | str, user_id: int | str
    ) -> dict[str, Any]:
        """GET /guilds/{guild_id}/members/{user_id} — Get a specific guild member."""
        return await self.request(
            self._route(
                "GET",
                "/guilds/{guild_id}/members/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            )
        )

    async def get_guild_members(
        self, guild_id: int | str, *, limit: int = 100, after: int | str | None = None
    ) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/members — List guild members."""
        params: dict[str, Any] = {"limit": limit}
        if after:
            params["after"] = after

        return await self.request(
            self._route("GET", "/guilds/{guild_id}/members", guild_id=guild_id),
            params=params,
        )

    async def create_guild(
        self,
        *,
        name: str,
        icon: bytes | None = None,
    ) -> dict[str, Any]:
        """POST /guilds — Create a new guild.

        Args:
            name: Guild name (2-100 characters)
            icon: Icon image data (PNG/JPG/GIF)

        Returns:
            Guild object
        """
        import base64

        payload: dict[str, Any] = {"name": name}

        if icon:
            # Convert bytes to base64 data URI
            image_data = base64.b64encode(icon).decode("ascii")
            # Detect image format from header
            if icon.startswith(b"\x89PNG"):
                mime_type = "image/png"
            elif icon.startswith(b"\xff\xd8\xff"):
                mime_type = "image/jpeg"
            elif icon.startswith(b"GIF89a") or icon.startswith(b"GIF87a"):
                mime_type = "image/gif"
            else:
                mime_type = "image/png"  # Default

            payload["icon"] = f"data:{mime_type};base64,{image_data}"

        return await self.request(self._route("POST", "/guilds"), json=payload)

    async def delete_guild(self, guild_id: int | str) -> None:
        """DELETE /guilds/{guild_id}"""
        await self.request(
            self._route("DELETE", "/guilds/{guild_id}", guild_id=guild_id)
        )

    async def modify_guild(
        self,
        guild_id: int | str,
        *,
        name: str | None = None,
        icon: bytes | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id} — Modify guild settings."""
        import base64

        payload: dict[str, Any] = {}

        if name is not None:
            payload["name"] = name

        if icon is not None:
            image_data = base64.b64encode(icon).decode("ascii")
            if icon.startswith(b"\x89PNG"):
                mime_type = "image/png"
            elif icon.startswith(b"\xff\xd8\xff"):
                mime_type = "image/jpeg"
            else:
                mime_type = "image/png"

            payload["icon"] = f"data:{mime_type};base64,{image_data}"

        payload.update(kwargs)

        return await self.request(
            self._route("PATCH", "/guilds/{guild_id}", guild_id=guild_id), json=payload
        )

    # -- Roles --
    async def get_guild_roles(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/roles"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/roles", guild_id=guild_id)
        )

    async def create_guild_role(
        self,
        guild_id: int | str,
        *,
        name: str | None = None,
        permissions: int | None = None,
        color: int = 0,
        hoist: bool = False,
        mentionable: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/roles — Create a new role."""
        payload: dict[str, Any] = {
            "color": color,
            "hoist": hoist,
            "mentionable": mentionable,
        }

        if name is not None:
            payload["name"] = name
        if permissions is not None:
            payload["permissions"] = str(permissions)

        payload.update(kwargs)

        return await self.request(
            self._route("POST", "/guilds/{guild_id}/roles", guild_id=guild_id),
            json=payload,
        )

    async def modify_guild_role(
        self,
        guild_id: int | str,
        role_id: int | str,
        *,
        name: str | None = None,
        permissions: int | None = None,
        color: int | None = None,
        hoist: bool | None = None,
        mentionable: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id}/roles/{role_id}"""
        payload: dict[str, Any] = {}

        if name is not None:
            payload["name"] = name
        if permissions is not None:
            payload["permissions"] = str(permissions)
        if color is not None:
            payload["color"] = color
        if hoist is not None:
            payload["hoist"] = hoist
        if mentionable is not None:
            payload["mentionable"] = mentionable

        payload.update(kwargs)

        return await self.request(
            self._route(
                "PATCH",
                "/guilds/{guild_id}/roles/{role_id}",
                guild_id=guild_id,
                role_id=role_id,
            ),
            json=payload,
        )

    async def delete_guild_role(self, guild_id: int | str, role_id: int | str) -> None:
        """DELETE /guilds/{guild_id}/roles/{role_id}"""
        await self.request(
            self._route(
                "DELETE",
                "/guilds/{guild_id}/roles/{role_id}",
                guild_id=guild_id,
                role_id=role_id,
            )
        )

    # -- Member Role Management --
    async def add_guild_member_role(
        self,
        guild_id: int | str,
        user_id: int | str,
        role_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """PUT /guilds/{guild_id}/members/{user_id}/roles/{role_id} — Add a role to a member.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID
            role_id: Role ID to add
            reason: Reason for audit log

        Returns:
            None (204 No Content)
        """
        await self.request(
            self._route(
                "PUT",
                "/guilds/{guild_id}/members/{user_id}/roles/{role_id}",
                guild_id=guild_id,
                user_id=user_id,
                role_id=role_id,
            ),
            reason=reason,
        )

    async def remove_guild_member_role(
        self,
        guild_id: int | str,
        user_id: int | str,
        role_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """DELETE /guilds/{guild_id}/members/{user_id}/roles/{role_id} — Remove a role from a member.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID
            role_id: Role ID to remove
            reason: Reason for audit log

        Returns:
            None (204 No Content)
        """
        await self.request(
            self._route(
                "DELETE",
                "/guilds/{guild_id}/members/{user_id}/roles/{role_id}",
                guild_id=guild_id,
                user_id=user_id,
                role_id=role_id,
            ),
            reason=reason,
        )

    # -- Moderation --
    async def kick_guild_member(
        self,
        guild_id: int | str,
        user_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """DELETE /guilds/{guild_id}/members/{user_id} — Remove (kick) a member from a guild.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID to kick
            reason: Reason for audit log

        Returns:
            None (204 No Content)
        """
        await self.request(
            self._route(
                "DELETE",
                "/guilds/{guild_id}/members/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            ),
            reason=reason,
        )

    async def ban_guild_member(
        self,
        guild_id: int | str,
        user_id: int | str,
        *,
        ban_duration_seconds: int = 0,
        delete_message_days: int = 0,
        delete_message_seconds: int = 0,
        reason: str | None = None,
    ) -> None:
        """PUT /guilds/{guild_id}/bans/{user_id} — Ban a member from a guild.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID to ban
            ban_duration_seconds: Duration of the ban in seconds (0 for permanent, or a valid temporary duration)
            delete_message_days: Number of days to delete messages for (0-7, deprecated)
            delete_message_seconds: Number of seconds to delete messages for (0-604800)
            reason: Reason for audit log

        Returns:
            None (204 No Content)
        """
        payload: dict[str, Any] = {}
        if ban_duration_seconds > 0:
            payload["ban_duration_seconds"] = ban_duration_seconds
        if delete_message_days > 0:
            payload["delete_message_days"] = delete_message_days
        if delete_message_seconds > 0:
            payload["delete_message_seconds"] = delete_message_seconds

        await self.request(
            self._route(
                "PUT",
                "/guilds/{guild_id}/bans/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            ),
            json=payload if payload else None,
            reason=reason,
        )

    async def unban_guild_member(
        self,
        guild_id: int | str,
        user_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """DELETE /guilds/{guild_id}/bans/{user_id} — Unban a user from a guild.

        Args:
            guild_id: Guild ID
            user_id: User ID to unban
            reason: Reason for audit log

        Returns:
            None (204 No Content)
        """
        await self.request(
            self._route(
                "DELETE",
                "/guilds/{guild_id}/bans/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            ),
            reason=reason,
        )

    async def timeout_guild_member(
        self,
        guild_id: int | str,
        user_id: int | str,
        *,
        until: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id}/members/{user_id} — Timeout (or remove timeout from) a member.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID to timeout
            until: ISO 8601 timestamp for when timeout expires (None to remove timeout)
            reason: Reason for audit log

        Returns:
            Updated member object
        """
        payload: dict[str, Any] = {
            "communication_disabled_until": until,
        }

        return await self.request(
            self._route(
                "PATCH",
                "/guilds/{guild_id}/members/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            ),
            json=payload,
            reason=reason,
        )

    async def modify_guild_member(
        self,
        guild_id: int | str,
        user_id: int | str,
        *,
        nick: str | None = None,
        roles: list[int | str] | None = None,
        mute: bool | None = None,
        deaf: bool | None = None,
        channel_id: int | str | None = None,
        communication_disabled_until: str | None = None,
        reason: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id}/members/{user_id} — Modify a guild member.

        Args:
            guild_id: Guild ID
            user_id: User/Member ID to modify
            nick: New nickname (None to remove)
            roles: List of role IDs to set (replaces all roles)
            mute: Whether to mute in voice channels
            deaf: Whether to deafen in voice channels
            channel_id: Voice channel to move member to
            communication_disabled_until: Timeout timestamp (ISO 8601)
            reason: Reason for audit log

        Returns:
            Updated member object
        """
        payload: dict[str, Any] = {}

        if nick is not None:
            payload["nick"] = nick
        if roles is not None:
            payload["roles"] = [str(r) for r in roles]
        if mute is not None:
            payload["mute"] = mute
        if deaf is not None:
            payload["deaf"] = deaf
        if channel_id is not None:
            payload["channel_id"] = str(channel_id)
        if communication_disabled_until is not None:
            payload["communication_disabled_until"] = communication_disabled_until

        payload.update(kwargs)

        return await self.request(
            self._route(
                "PATCH",
                "/guilds/{guild_id}/members/{user_id}",
                guild_id=guild_id,
                user_id=user_id,
            ),
            json=payload,
            reason=reason,
        )

    # -- Channels (create/modify) --
    async def create_guild_channel(
        self,
        guild_id: int | str,
        *,
        name: str,
        type: int = 0,
        topic: str | None = None,
        bitrate: int | None = None,
        user_limit: int | None = None,
        position: int | None = None,
        parent_id: int | str | None = None,
        nsfw: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/channels — Create a channel.

        Args:
            guild_id: Guild to create channel in
            name: Channel name
            type: Channel type (0=text, 2=voice, 4=category)
            topic: Channel topic (text channels)
            bitrate: Bitrate (voice channels)
            user_limit: User limit (voice channels)
            position: Channel position
            parent_id: Parent category ID
            nsfw: Whether the channel is NSFW

        Returns:
            Channel object
        """
        payload: dict[str, Any] = {
            "name": name,
            "type": type,
            "nsfw": nsfw,
        }

        if topic is not None:
            payload["topic"] = topic
        if bitrate is not None:
            payload["bitrate"] = bitrate
        if user_limit is not None:
            payload["user_limit"] = user_limit
        if position is not None:
            payload["position"] = position
        if parent_id is not None:
            payload["parent_id"] = parent_id

        payload.update(kwargs)

        return await self.request(
            self._route("POST", "/guilds/{guild_id}/channels", guild_id=guild_id),
            json=payload,
        )

    async def modify_channel(
        self,
        channel_id: int | str,
        *,
        name: str | None = None,
        type: int | None = None,
        topic: str | None = None,
        position: int | None = None,
        parent_id: int | str | None = None,
        nsfw: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """PATCH /channels/{channel_id}"""
        payload: dict[str, Any] = {}

        if name is not None:
            payload["name"] = name
        if type is not None:
            payload["type"] = type
        if topic is not None:
            payload["topic"] = topic
        if position is not None:
            payload["position"] = position
        if parent_id is not None:
            payload["parent_id"] = parent_id
        if nsfw is not None:
            payload["nsfw"] = nsfw

        payload.update(kwargs)

        return await self.request(
            self._route("PATCH", "/channels/{channel_id}", channel_id=channel_id),
            json=payload,
        )

    async def delete_channel(self, channel_id: int | str) -> None:
        """DELETE /channels/{channel_id}"""
        await self.request(
            self._route("DELETE", "/channels/{channel_id}", channel_id=channel_id)
        )

    async def edit_channel_permissions(
        self,
        channel_id: int | str,
        overwrite_id: int | str,
        *,
        allow: int | str | None = None,
        deny: int | str | None = None,
        type: int = 0,
        **kwargs: Any,
    ) -> None:
        """PUT /channels/{channel_id}/permissions/{overwrite_id} — Edit channel permission overwrites.

        Args:
            channel_id: Channel ID
            overwrite_id: Role or user ID
            allow: Allowed permissions (bitwise)
            deny: Denied permissions (bitwise)
            type: 0 for role, 1 for member

        Returns:
            None (204 No Content)
        """
        payload: dict[str, Any] = {"type": type}

        if allow is not None:
            payload["allow"] = str(allow)
        if deny is not None:
            payload["deny"] = str(deny)

        payload.update(kwargs)

        await self.request(
            self._route(
                "PUT",
                "/channels/{channel_id}/permissions/{overwrite_id}",
                channel_id=channel_id,
                overwrite_id=overwrite_id,
            ),
            json=payload,
        )

    # -- User Profile --
    async def modify_current_user(
        self,
        *,
        username: str | None = None,
        avatar: bytes | None = None,
        banner: bytes | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """PATCH /users/@me — Modify the current user's profile.

        Args:
            username: New username
            avatar: Avatar image data (PNG/JPG/GIF)
            banner: Banner image data (PNG/JPG/GIF)

        Returns:
            Updated user object
        """
        import base64

        payload: dict[str, Any] = {}

        if username is not None:
            payload["username"] = username

        if avatar is not None:
            image_data = base64.b64encode(avatar).decode("ascii")
            if avatar.startswith(b"\x89PNG"):
                mime_type = "image/png"
            elif avatar.startswith(b"\xff\xd8\xff"):
                mime_type = "image/jpeg"
            elif avatar.startswith(b"GIF89a") or avatar.startswith(b"GIF87a"):
                mime_type = "image/gif"
            else:
                mime_type = "image/png"

            payload["avatar"] = f"data:{mime_type};base64,{image_data}"

        if banner is not None:
            image_data = base64.b64encode(banner).decode("ascii")
            if banner.startswith(b"\x89PNG"):
                mime_type = "image/png"
            elif banner.startswith(b"\xff\xd8\xff"):
                mime_type = "image/jpeg"
            elif banner.startswith(b"GIF89a") or banner.startswith(b"GIF87a"):
                mime_type = "image/gif"
            else:
                mime_type = "image/png"

            payload["banner"] = f"data:{mime_type};base64,{image_data}"

        payload.update(kwargs)

        return await self.request(self._route("PATCH", "/users/@me"), json=payload)

    # -- Emojis --
    async def get_guild_emojis(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/emojis — Get all emojis for a guild.

        Returns:
            List of emoji objects
        """
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/emojis", guild_id=guild_id)
        )

    async def get_guild_emoji(
        self, guild_id: int | str, emoji_id: int | str
    ) -> dict[str, Any]:
        """GET /guilds/{guild_id}/emojis/{emoji_id} — Get a specific emoji.

        Returns:
            Emoji object
        """
        return await self.request(
            self._route(
                "GET",
                "/guilds/{guild_id}/emojis/{emoji_id}",
                guild_id=guild_id,
                emoji_id=emoji_id,
            )
        )

    async def create_guild_emoji(
        self,
        guild_id: int | str,
        *,
        name: str,
        image: bytes,
        roles: list[int | str] | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/emojis — Create a new emoji.

        Args:
            guild_id: Guild ID
            name: Emoji name
            image: Image data (PNG/JPG/GIF)
            roles: List of role IDs that can use this emoji (optional)
            reason: Reason for creation (audit log)

        Returns:
            Emoji object
        """
        import base64

        # Convert bytes to base64 data URI
        image_data = base64.b64encode(image).decode("ascii")

        # Detect image format from header
        if image.startswith(b"\x89PNG"):
            mime_type = "image/png"
        elif image.startswith(b"\xff\xd8\xff"):
            mime_type = "image/jpeg"
        elif image.startswith(b"GIF89a") or image.startswith(b"GIF87a"):
            mime_type = "image/gif"
        else:
            mime_type = "image/png"  # Default

        payload: dict[str, Any] = {
            "name": name,
            "image": f"data:{mime_type};base64,{image_data}",
        }

        if roles is not None:
            payload["roles"] = [str(role_id) for role_id in roles]

        return await self.request(
            self._route("POST", "/guilds/{guild_id}/emojis", guild_id=guild_id),
            json=payload,
            reason=reason,
        )

    async def delete_guild_emoji(
        self,
        guild_id: int | str,
        emoji_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """DELETE /guilds/{guild_id}/emojis/{emoji_id} — Delete an emoji.

        Args:
            guild_id: Guild ID
            emoji_id: Emoji ID
            reason: Reason for deletion (audit log)
        """
        await self.request(
            self._route(
                "DELETE",
                "/guilds/{guild_id}/emojis/{emoji_id}",
                guild_id=guild_id,
                emoji_id=emoji_id,
            ),
            reason=reason,
        )

    # -- Stickers --
    async def get_guild_stickers(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/stickers — Get all stickers for a guild.

        Returns:
            List of emoji objects
        """
        return await self.request(
            Route("GET", "/guilds/{guild_id}/stickers", guild_id=guild_id)
        )

    async def get_guild_sticker(
        self, guild_id: int | str, sticker_id: int | str
    ) -> dict[str, Any]:
        """GET /guilds/{guild_id}/sticker/{sticker_id} — Get a specific sticker.

        Returns:
            Sticker object
        """
        return await self.request(
            Route(
                "GET",
                "/guilds/{guild_id}/sticker/{sticker_id}",
                guild_id=guild_id,
                sticker_id=sticker_id,
            )
        )

    async def create_guild_sticker(
        self,
        guild_id: int | str,
        *,
        name: str,
        image: bytes,
        roles: list[int | str] | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/stickers — Create a new sticker.

        Args:
            guild_id: Guild ID
            name: Sticker name
            image: Image data (PNG/JPG/GIF)
            roles: List of role IDs that can use this sticker (optional)
            reason: Reason for creation (audit log)

        Returns:
            Sticker object
        """
        import base64

        # Convert bytes to base64 data URI
        image_data = base64.b64encode(image).decode("ascii")

        # Detect image format from header
        if image.startswith(b"\x89PNG"):
            mime_type = "image/png"
        elif image.startswith(b"\xff\xd8\xff"):
            mime_type = "image/jpeg"
        elif image.startswith(b"GIF89a") or image.startswith(b"GIF87a"):
            mime_type = "image/gif"
        else:
            mime_type = "image/png"  # Default

        payload: dict[str, Any] = {
            "name": name,
            "image": f"data:{mime_type};base64,{image_data}",
        }

        if roles is not None:
            payload["roles"] = [str(role_id) for role_id in roles]

        return await self.request(
            Route("POST", "/guilds/{guild_id}/stickers", guild_id=guild_id),
            json=payload,
            reason=reason,
        )

    async def delete_guild_sticker(
        self,
        guild_id: int | str,
        sticker_id: int | str,
        *,
        reason: str | None = None,
    ) -> None:
        """DELETE /guilds/{guild_id}/stickers/{sticker_id} — Delete an sticker.

        Args:
            guild_id: Guild ID
            sticker_id: Sticker ID
            reason: Reason for deletion (audit log)
        """
        await self.request(
            Route(
                "DELETE",
                "/guilds/{guild_id}/stickers/{stickers_id}",
                guild_id=guild_id,
                sticker_id=sticker_id,
            ),
            reason=reason,
        )

    async def get_guild_discovery_status(self, guild_id: int | str) -> dict[str, Any]:
        """GET /guilds/{guild_id}/discovery"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/discovery", guild_id=guild_id)
        )

    async def apply_for_guild_discovery(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/discovery"""
        return await self.request(
            self._route("POST", "/guilds/{guild_id}/discovery", guild_id=guild_id),
            json=payload,
        )

    async def edit_guild_discovery_application(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id}/discovery"""
        return await self.request(
            self._route("PATCH", "/guilds/{guild_id}/discovery", guild_id=guild_id),
            json=payload,
        )

    async def apply_for_discovery(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """Alias for applying a guild to Fluxer discovery."""
        return await self.apply_for_guild_discovery(guild_id, **payload)

    async def edit_discovery_application(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """Alias for editing a guild discovery application."""
        return await self.edit_guild_discovery_application(guild_id, **payload)

    async def withdraw_discovery_application(self, guild_id: int | str) -> None:
        """DELETE /guilds/{guild_id}/discovery"""
        await self.request(
            self._route("DELETE", "/guilds/{guild_id}/discovery", guild_id=guild_id)
        )

    async def join_discovery_guild(self, guild_id: int | str) -> Any:
        """POST /discovery/guilds/{guild_id}/join"""
        return await self.request(
            self._route("POST", "/discovery/guilds/{guild_id}/join", guild_id=guild_id)
        )

    async def get_guild_vanity_url(self, guild_id: int | str) -> dict[str, Any]:
        """GET /guilds/{guild_id}/vanity-url"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/vanity-url", guild_id=guild_id)
        )

    async def update_guild_vanity_url(
        self, guild_id: int | str, code: str
    ) -> dict[str, Any]:
        """PATCH /guilds/{guild_id}/vanity-url"""
        return await self.request(
            self._route("PATCH", "/guilds/{guild_id}/vanity-url", guild_id=guild_id),
            json={"code": code},
        )

    async def transfer_guild_ownership(
        self, guild_id: int | str, new_owner_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/transfer-ownership"""
        payload["owner_id"] = str(new_owner_id)
        return await self.request(
            self._route(
                "POST", "/guilds/{guild_id}/transfer-ownership", guild_id=guild_id
            ),
            json=payload,
        )

    async def bulk_create_guild_emojis(
        self, guild_id: int | str, emojis: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/emojis/bulk"""
        return await self.request(
            self._route("POST", "/guilds/{guild_id}/emojis/bulk", guild_id=guild_id),
            json={"emojis": emojis},
        )

    async def bulk_create_guild_stickers(
        self, guild_id: int | str, stickers: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/stickers/bulk"""
        return await self.request(
            self._route("POST", "/guilds/{guild_id}/stickers/bulk", guild_id=guild_id),
            json={"stickers": stickers},
        )

    async def clone_guild_emoji(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/emojis/clone"""
        return await self.request(
            self._route("POST", "/guilds/{guild_id}/emojis/clone", guild_id=guild_id),
            json=payload,
        )

    async def clone_guild_sticker(
        self, guild_id: int | str, **payload: Any
    ) -> dict[str, Any]:
        """POST /guilds/{guild_id}/stickers/clone"""
        return await self.request(
            self._route("POST", "/guilds/{guild_id}/stickers/clone", guild_id=guild_id),
            json=payload,
        )

    # ~~ Webhooks ~
    async def get_guild_webhooks(self, guild_id: int | str) -> list[dict[str, Any]]:
        """GET /guilds/{guild_id}/webhooks"""
        return await self.request(
            self._route("GET", "/guilds/{guild_id}/webhooks", guild_id=guild_id)
        )

    async def get_channel_webhooks(self, channel_id: int | str) -> list[dict[str, Any]]:
        """GET /channels/{channel_id}/webhooks"""
        return await self.request(
            self._route("GET", "/channels/{channel_id}/webhooks", channel_id=channel_id)
        )

    async def create_webhook(
        self,
        channel_id: int | str,
        *,
        name: str,
        avatar: str | None = None,
    ) -> dict[str, Any]:
        """POST /channels/{channel_id}/webhooks"""
        payload: dict[str, Any] = {"name": name}
        if avatar is not None:
            payload["avatar"] = avatar
        return await self.request(
            self._route(
                "POST", "/channels/{channel_id}/webhooks", channel_id=channel_id
            ),
            json=payload,
        )

    async def get_webhook(self, webhook_id: int | str) -> dict[str, Any]:
        """GET /webhooks/{webhook_id}"""
        return await self.request(
            self._route("GET", "/webhooks/{webhook_id}", webhook_id=webhook_id)
        )

    async def get_webhook_with_token(
        self, webhook_id: int | str, token: str
    ) -> dict[str, Any]:
        """GET /webhooks/{webhook_id}/{token}"""
        return await self.request(
            self._route(
                "GET",
                "/webhooks/{webhook_id}/{token}",
                webhook_id=webhook_id,
                token=token,
            )
        )

    async def modify_webhook(
        self,
        webhook_id: int | str,
        *,
        name: str | None = None,
        avatar: str | None = None,
        channel_id: int | str | None = None,
    ) -> dict[str, Any]:
        """PATCH /webhooks/{webhook_id}"""
        payload: dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if avatar is not None:
            payload["avatar"] = avatar
        if channel_id is not None:
            payload["channel_id"] = channel_id
        return await self.request(
            self._route("PATCH", "/webhooks/{webhook_id}", webhook_id=webhook_id),
            json=payload,
        )

    async def modify_webhook_with_token(
        self,
        webhook_id: int | str,
        token: str,
        *,
        name: str | None = None,
        avatar: str | None = None,
        channel_id: int | str | None = None,
    ) -> dict[str, Any]:
        """PATCH /webhooks/{webhook_id}/{token}"""
        payload: dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if avatar is not None:
            payload["avatar"] = avatar
        if channel_id is not None:
            payload["channel_id"] = channel_id
        return await self.request(
            self._route(
                "PATCH",
                "/webhooks/{webhook_id}/{token}",
                webhook_id=webhook_id,
                token=token,
            ),
            json=payload,
        )

    async def delete_webhook(
        self, webhook_id: int | str, *, reason: str | None = None
    ) -> None:
        """DELETE /webhooks/{webhook_id}"""
        await self.request(
            self._route("DELETE", "/webhooks/{webhook_id}", webhook_id=webhook_id),
            reason=reason,
        )

    async def delete_webhook_with_token(
        self, webhook_id: int | str, token: str
    ) -> None:
        """DELETE /webhooks/{webhook_id}/{token}"""
        await self.request(
            self._route(
                "DELETE",
                "/webhooks/{webhook_id}/{token}",
                webhook_id=webhook_id,
                token=token,
            ),
        )

    async def execute_webhook(
        self,
        webhook_id: int | str,
        token: str,
        *,
        content: str | None = None,
        embeds: list[dict[str, Any]] | None = None,
        username: str | None = None,
        avatar_url: str | None = None,
        wait: bool = False,
        files: list[dict[str, Any]] | None = None,
        allowed_mentions: Any | None = None,
        message_reference: Any | None = None,
        flags: int | None = None,
        nonce: str | int | None = None,
        favorite_meme_id: int | str | None = None,
        sticker_ids: list[int | str] | None = None,
        tts: bool | None = None,
    ) -> dict[str, Any] | None:
        """POST /webhooks/{webhook_id}/{token}"""
        route = self._route(
            "POST",
            "/webhooks/{webhook_id}/{token}",
            webhook_id=webhook_id,
            token=token,
        )
        payload: dict[str, Any] = {}
        if content is not None:
            payload["content"] = content
        if embeds is not None:
            payload["embeds"] = embeds
        if username is not None:
            payload["username"] = username
        if avatar_url is not None:
            payload["avatar_url"] = avatar_url
        if allowed_mentions is not None:
            payload["allowed_mentions"] = _payload_value(allowed_mentions)
        if message_reference is not None:
            payload["message_reference"] = _payload_value(message_reference)
        if flags is not None:
            payload["flags"] = flags
        if nonce is not None:
            payload["nonce"] = nonce
        if favorite_meme_id is not None:
            payload["favorite_meme_id"] = str(favorite_meme_id)
        if sticker_ids is not None:
            payload["sticker_ids"] = [str(sticker_id) for sticker_id in sticker_ids]
        if tts is not None:
            payload["tts"] = tts
        params = {"wait": "true"} if wait else None
        if files:
            form = aiohttp.FormData()
            payload["attachments"] = [
                {"id": i, "filename": file["filename"]} for i, file in enumerate(files)
            ]
            form.add_field(
                "payload_json",
                json_mod.dumps(payload),
                content_type="application/json",
            )
            for i, file in enumerate(files):
                form.add_field(
                    f"files[{i}]",
                    file["data"],
                    filename=file["filename"],
                )
            return await self.request(route, data=form, params=params)
        return await self.request(
            route,
            json=payload,
            params=params,
        )

    async def edit_webhook_message(
        self,
        webhook_id: int | str,
        token: str,
        message_id: int | str,
        *,
        content: str | None = None,
        embeds: list[dict[str, Any]] | None = None,
        allowed_mentions: Any | None = None,
        flags: int | None = None,
    ) -> dict[str, Any]:
        """PATCH /webhooks/{webhook_id}/{token}/messages/{message_id}"""
        route = self._route(
            "PATCH",
            "/webhooks/{webhook_id}/{token}/messages/{message_id}",
            webhook_id=webhook_id,
            token=token,
            message_id=message_id,
        )
        payload: dict[str, Any] = {}
        if content is not None:
            payload["content"] = content
        if embeds is not None:
            payload["embeds"] = embeds
        if allowed_mentions is not None:
            payload["allowed_mentions"] = _payload_value(allowed_mentions)
        if flags is not None:
            payload["flags"] = flags
        return await self.request(route, json=payload)

    async def delete_webhook_message(
        self,
        webhook_id: int | str,
        token: str,
        message_id: int | str,
    ) -> None:
        """DELETE /webhooks/{webhook_id}/{token}/messages/{message_id}"""
        await self.request(
            self._route(
                "DELETE",
                "/webhooks/{webhook_id}/{token}/messages/{message_id}",
                webhook_id=webhook_id,
                token=token,
                message_id=message_id,
            )
        )

    async def execute_github_webhook(
        self, webhook_id: int | str, token: str, payload: dict[str, Any]
    ) -> Any:
        """POST /webhooks/{webhook_id}/{token}/github"""
        return await self.request(
            self._route(
                "POST",
                "/webhooks/{webhook_id}/{token}/github",
                webhook_id=webhook_id,
                token=token,
            ),
            json=payload,
        )

    async def execute_instatus_webhook(
        self, webhook_id: int | str, token: str, payload: dict[str, Any]
    ) -> Any:
        """POST /webhooks/{webhook_id}/{token}/instatus"""
        return await self.request(
            self._route(
                "POST",
                "/webhooks/{webhook_id}/{token}/instatus",
                webhook_id=webhook_id,
                token=token,
            ),
            json=payload,
        )

    async def execute_slack_webhook(
        self, webhook_id: int | str, token: str, payload: dict[str, Any]
    ) -> Any:
        """POST /webhooks/{webhook_id}/{token}/slack"""
        return await self.request(
            self._route(
                "POST",
                "/webhooks/{webhook_id}/{token}/slack",
                webhook_id=webhook_id,
                token=token,
            ),
            json=payload,
        )

    # -- Reactions --
    def _emoji_to_url_format(self, emoji: Any) -> str:
        """Convert an emoji object to URL format for reaction endpoints.

        Args:
            emoji: PartialEmoji, Emoji, or str (unicode emoji or :shortcode:)

        Returns:
            URL-encoded emoji string or "name:id" for custom emojis
        """
        import re
        import emoji as emoji_lib
        import urllib.parse

        # Handle PartialEmoji or Emoji objects
        if hasattr(emoji, "id") and emoji.id:
            # Custom emoji: name:id (no encoding needed)
            return f"{emoji.name}:{emoji.id}"
        elif hasattr(emoji, "name") and emoji.name:
            # Unicode emoji from PartialEmoji
            return urllib.parse.quote(emoji.name, safe="")
        else:
            # Handle string emojis
            emoji_str = str(emoji)

            # Check for custom emoji format: <:name:id> or <a:name:id>
            custom_emoji_match = re.match(r"^<a?:([^:]+):(\d+)>$", emoji_str)
            if custom_emoji_match:
                # Extract name and id, return as name:id
                name, emoji_id = custom_emoji_match.groups()
                return f"{name}:{emoji_id}"

            # Convert shortcode to unicode emoji (e.g., :joy: → 😂)
            # emojize will convert :joy: to 😂, but leave 😂 as-is
            emoji_str = emoji_lib.emojize(emoji_str, language="alias")

            # URL-encode the result
            return urllib.parse.quote(emoji_str, safe="")

    async def add_reaction(
        self,
        channel_id: int | str,
        message_id: int | str,
        emoji: Any,
    ) -> None:
        """PUT /channels/{channel_id}/messages/{message_id}/reactions/{emoji}/@me

        Add a reaction to a message.

        Args:
            channel_id: Channel ID
            message_id: Message ID
            emoji: Emoji to react with (PartialEmoji, Emoji, or unicode string)
        """
        emoji_str = self._emoji_to_url_format(emoji)
        await self.request(
            self._route(
                "PUT",
                "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}/@me",
                channel_id=channel_id,
                message_id=message_id,
                emoji=emoji_str,
            )
        )

    async def delete_reaction(
        self,
        channel_id: int | str,
        message_id: int | str,
        emoji: Any,
        user_id: int | str = "@me",
    ) -> None:
        """DELETE /channels/{channel_id}/messages/{message_id}/reactions/{emoji}/{user_id}

        Remove a reaction from a message.

        Args:
            channel_id: Channel ID
            message_id: Message ID
            emoji: Emoji to remove (PartialEmoji, Emoji, or unicode string)
            user_id: User ID to remove reaction from (default: @me)
        """
        emoji_str = self._emoji_to_url_format(emoji)
        await self.request(
            self._route(
                "DELETE",
                "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}/{user_id}",
                channel_id=channel_id,
                message_id=message_id,
                emoji=emoji_str,
                user_id=user_id,
            )
        )

    async def get_reaction_users(
        self,
        channel_id: int | str,
        message_id: int | str,
        emoji: Any,
        *,
        limit: int = 25,
        after: int | str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /channels/{channel_id}/messages/{message_id}/reactions/{emoji}

        Get users who reacted with a specific emoji.

        Args:
            channel_id: Channel ID
            message_id: Message ID
            emoji: Emoji to get users for (PartialEmoji, Emoji, or unicode string)
            limit: Max number of users to return (1-100, default 25)
            after: Get users after this user ID

        Returns:
            List of user objects
        """
        emoji_str = self._emoji_to_url_format(emoji)
        params: dict[str, Any] = {"limit": limit}
        if after:
            params["after"] = after

        return await self.request(
            self._route(
                "GET",
                "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}",
                channel_id=channel_id,
                message_id=message_id,
                emoji=emoji_str,
            ),
            params=params,
        )

    async def delete_all_reactions(
        self,
        channel_id: int | str,
        message_id: int | str,
    ) -> None:
        """DELETE /channels/{channel_id}/messages/{message_id}/reactions

        Remove all reactions from a message.

        Args:
            channel_id: Channel ID
            message_id: Message ID
        """
        await self.request(
            self._route(
                "DELETE",
                "/channels/{channel_id}/messages/{message_id}/reactions",
                channel_id=channel_id,
                message_id=message_id,
            )
        )

    async def delete_all_reactions_for_emoji(
        self,
        channel_id: int | str,
        message_id: int | str,
        emoji: Any,
    ) -> None:
        """DELETE /channels/{channel_id}/messages/{message_id}/reactions/{emoji}

        Remove all reactions of a specific emoji from a message.

        Args:
            channel_id: Channel ID
            message_id: Message ID
            emoji: Emoji to remove all reactions for (PartialEmoji, Emoji, or unicode string)
        """
        emoji_str = self._emoji_to_url_format(emoji)
        await self.request(
            self._route(
                "DELETE",
                "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}",
                channel_id=channel_id,
                message_id=message_id,
                emoji=emoji_str,
            )
        )
