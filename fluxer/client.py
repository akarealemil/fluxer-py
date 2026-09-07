from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
import logging
import sys
from collections.abc import Awaitable, Iterable
from typing import TYPE_CHECKING, Any, Callable, Coroutine, TypeVar

if TYPE_CHECKING:
    from .activity import BaseActivity
    from .voice import VoiceClient

from .enums import Intents
from .errors import GatewayNotConnected
from .events import fluxer_event_from_dispatch
from .fluxer_models import (
    SearchAuthorType,
    SearchContentType,
    SearchEmbedType,
    SearchResponse,
    SearchSortBy,
    SearchSortOrder,
    parse_search_response,
)
from .gateway import Gateway
from .http import HTTPClient
from .models import Channel, Guild, Message, User, UserProfile, VoiceState, Webhook
from .models.role import Role
from .state import ConnectionState

log = logging.getLogger(__name__)

# Type alias for event handlers
EventHandler = Callable[..., Coroutine[Any, Any, None]]


class Client:
    """Low-level client that connects to Fluxer and dispatches events.

    This gives you full control over the gateway lifecycle.
    For most bots, use the Bot subclass instead.

    Args:
        intents: Gateway intents to request (default: Intents.default())
        api_url: Base URL for the Fluxer API (default: https://api.fluxer.app/v1)
                 Use this to connect to self-hosted Fluxer instances
    """

    def __init__(
        self,
        *,
        intents: Intents = Intents.default(),
        api_url: str | None = None,
        max_retries: int = 5,
        retry_forever: bool = False,
        max_messages: int = 1000,
        cache_members: bool = True,
    ) -> None:
        self.intents = intents
        self.api_url = api_url
        self._http: HTTPClient | None = None
        self._gateway: Gateway | None = None
        self._event_handlers: dict[str, list[EventHandler]] = {}
        self._user: User | None = None
        self._state = ConnectionState(
            max_messages=max_messages, cache_members=cache_members
        )
        self._guilds = self._state.guilds
        self._channels = self._state.channels
        self._voice_states = self._state.voice_states
        self._pending_voice: dict[int, VoiceClient] = {}
        self._closed: bool = False
        self._ready = asyncio.Event()
        self._waiters: dict[
            str, list[tuple[asyncio.Future[Any], Callable[..., bool] | None]]
        ] = {}
        self._max_retries = max_retries
        self._retry_forever = retry_forever

    @property
    def user(self) -> User | None:
        """The bot user, available after the READY event."""
        return self._user

    @property
    def guilds(self) -> list[Guild]:
        """List of guilds the bot is in (populated from READY + GUILD_CREATE)."""
        return list(self._guilds.values())

    def is_ready(self) -> bool:
        """Return whether the READY event has been received."""
        return self._ready.is_set()

    def is_closed(self) -> bool:
        """Return whether the client has been closed."""
        return self._closed

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the active asyncio event loop."""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.get_event_loop()

    async def wait_until_ready(self) -> None:
        """Wait until the client has received READY from the gateway."""
        await self._ready.wait()

    async def wait_for(
        self,
        event: str,
        *,
        check: Callable[..., bool] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Wait until a Fluxer event matching ``check`` is dispatched."""
        event_name = event[3:] if event.startswith("on_") else event
        future: asyncio.Future[Any] = self.loop.create_future()
        waiter = (future, check)
        self._waiters.setdefault(event_name, []).append(waiter)
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            waiters = self._waiters.get(event_name)
            if waiters and waiter in waiters:
                waiters.remove(waiter)
            if waiters == []:
                self._waiters.pop(event_name, None)

    def get_guild(self, id: int | str) -> Guild | None:
        try:
            return self._guilds.get(int(id))
        except (TypeError, ValueError):
            return None

    def get_channel(self, id: int | str) -> Channel | None:
        """Return a cached channel by ID, if available."""
        try:
            return self._channels.get(int(id))
        except (TypeError, ValueError):
            return None

    def get_member(self, guild_id: int | str | None, user_id: int | str) -> Any | None:
        """Return a cached guild member by guild and user ID, if available."""
        try:
            guild_key = int(guild_id) if guild_id is not None else None
            user_key = int(user_id)
        except (TypeError, ValueError):
            return None
        return self._state.get_member(guild_key, user_key)

    def get_message(self, message_id: int | str | None) -> Message | None:
        """Return a cached message by ID, if available."""
        return self._state.get_message(message_id)

    @property
    def cached_messages(self) -> list[Message]:
        """Messages currently retained by the in-memory message cache."""
        return list(self._state.messages.values())

    # =========================================================================
    # Event registration
    # =========================================================================

    def event(self, func: EventHandler) -> EventHandler:
        """Decorator to register an event handler.

        The function name determines the event:
            @bot.event
            async def on_message(message):
                ...

        Supported events (mapped from gateway dispatch names):
            on_ready       -> READY
            on_message      -> MESSAGE_CREATE
            on_message_edit -> MESSAGE_UPDATE
            on_message_delete -> MESSAGE_DELETE
            on_guild_join   -> GUILD_CREATE
            on_guild_remove -> GUILD_DELETE
            on_member_join  -> GUILD_MEMBER_ADD
            on_member_remove -> GUILD_MEMBER_REMOVE
            ... and any other gateway event as on_{lowercase_name}
        """
        event_name = func.__name__
        if not event_name.startswith("on_"):
            raise ValueError(f"Event handler must start with 'on_', got '{event_name}'")

        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(func)
        return func

    def on(self, event_name: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator to register an event handler with an explicit name.

        Usage:
            @bot.on("message")
            async def handle_msg(message):
                ...
        """

        def decorator(func: EventHandler) -> EventHandler:
            key = f"on_{event_name}"
            if key not in self._event_handlers:
                self._event_handlers[key] = []
            self._event_handlers[key].append(func)
            return func

        return decorator

    # =========================================================================
    # Event dispatching
    # =========================================================================

    async def _dispatch(self, event_name: str, data: Any) -> None:
        """Called by the Gateway when a dispatch event is received.

        This method:
        1. Parses the raw data into model objects
        2. Updates internal caches
        3. Fires matching user event handlers
        """
        # Map gateway event names to handler names and parse data
        match event_name:
            case "READY":
                self._user = User.from_data(data["user"], self._http)
                # Process guilds from READY
                for guild_data in data.get("guilds", []):
                    guild = Guild.from_data(guild_data, self._http)
                    self._state.store_guild(guild)
                    self._seed_guild_voice_states(
                        guild.id, guild_data.get("voice_states", [])
                    )
                self._ready.set()
                await self._fire("on_ready")

            case "MESSAGE_CREATE":
                message = self._parse_message(data)
                await self._fire("on_message", message)

            case "MESSAGE_UPDATE":
                message = self._parse_message(data)
                await self._fire("on_message_edit", message)

            case "MESSAGE_DELETE":
                await self._fire("on_message_delete", data)

            case "GUILD_CREATE":
                guild = Guild.from_data(data, self._http)
                self._state.store_guild(guild)
                # Cache channels from guild
                for ch_data in data.get("channels", []):
                    ch = Channel.from_data(ch_data, self._http)
                    ch._guild = guild
                    self._state.store_channel(ch)
                self._seed_guild_voice_states(guild.id, data.get("voice_states", []))
                await self._fire("on_guild_join", guild)

            case "GUILD_DELETE":
                guild_id = int(data["id"])
                guild = self._guilds.pop(guild_id, None)
                await self._fire("on_guild_remove", guild or data)

            case "GUILD_MEMBER_ADD":
                await self._fire("on_member_join", data)

            case "GUILD_MEMBER_REMOVE":
                await self._fire("on_member_remove", data)

            case "CHANNEL_CREATE":
                channel = Channel.from_data(data, self._http)
                if channel.guild_id is not None:
                    channel._guild = self._guilds.get(channel.guild_id)
                self._state.store_channel(channel)
                await self._fire("on_channel_create", channel)

            case "CHANNEL_UPDATE":
                channel = Channel.from_data(data, self._http)
                if channel.guild_id is not None:
                    channel._guild = self._guilds.get(channel.guild_id)
                self._state.store_channel(channel)
                await self._fire("on_channel_update", channel)

            case "CHANNEL_DELETE":
                # CHANNEL_DELETE only provides minimal data (guild_id, id)
                # Try to get the full channel from cache before removing it
                channel_id = int(data["id"])
                channel = self._channels.pop(channel_id, None)

                if channel:
                    # We have the full channel object from cache
                    await self._fire("on_channel_delete", channel)
                else:
                    # Channel wasn't cached, fire event with raw data
                    await self._fire("on_channel_delete", data)

            case "VOICE_STATE_UPDATE":
                voice_state = self._store_voice_state(data)
                await self._fire("on_voice_state_update", voice_state)

            case "VOICE_SERVER_UPDATE":
                guild_id = int(data["guild_id"])
                vc = self._pending_voice.pop(guild_id, None)
                if vc:
                    bot_user_id = self._user.id if self._user else None
                    cached_state = (
                        self.get_voice_state(guild_id, bot_user_id)
                        if bot_user_id is not None
                        else None
                    )
                    session_id = cached_state.session_id if cached_state else ""
                    await vc._on_voice_server_update(
                        data["endpoint"], data["token"], session_id or ""
                    )

            case "RESUMED":
                await self._fire("on_resumed")

            case "MESSAGE_REACTION_ADD":
                await self._handle_reaction_add(data)

            case "MESSAGE_REACTION_REMOVE":
                await self._handle_reaction_remove(data)

            case "MESSAGE_REACTION_REMOVE_ALL":
                await self._handle_reaction_remove_all(data)

            case "MESSAGE_REACTION_REMOVE_EMOJI":
                await self._handle_reaction_remove_emoji(data)

            case "CHANNEL_UPDATE_BULK":
                for channel_data in data.get("channels", []):
                    channel = Channel.from_data(channel_data, self._http)
                    if channel.guild_id is not None:
                        channel._guild = self._guilds.get(channel.guild_id)
                    self._state.store_channel(channel)
                await self._fire(
                    "on_fluxer_event", fluxer_event_from_dispatch(event_name, data)
                )

            case "GUILD_ROLE_UPDATE_BULK":
                guild_id = int(data["guild_id"])
                guild = self._guilds.get(guild_id)
                if guild is not None:
                    guild.roles = [
                        Role.from_data(role_data, self._http, guild_id)
                        for role_data in data.get("roles", [])
                    ]
                await self._fire(
                    "on_fluxer_event", fluxer_event_from_dispatch(event_name, data)
                )

            case _:
                # Unknown/unhandled event — fire a generic handler
                handler_name = f"on_{event_name.lower()}"
                await self._fire(handler_name, data)
                await self._fire(
                    "on_fluxer_event", fluxer_event_from_dispatch(event_name, data)
                )

    def _parse_message(self, data: dict[str, Any]) -> Message:
        """Parse message data and attach cached channel and guild references."""
        msg = Message.from_data(data, self._http)
        # Attach cached channel
        cached_channel = self._channels.get(msg.channel_id)
        if cached_channel:
            msg._channel = cached_channel
        # Resolve guild_id via channel if not present in message data
        guild_id = msg.guild_id or (cached_channel.guild_id if cached_channel else None)
        if guild_id is not None:
            cached_guild = self._guilds.get(guild_id)
            if cached_guild:
                msg._cache_guild(cached_guild)
        return self._state.store_message(msg)

    async def _handle_reaction_add(self, data: dict[str, Any]) -> None:
        """Handle MESSAGE_REACTION_ADD event."""
        from .models.reaction import RawReactionActionEvent

        raw = RawReactionActionEvent.from_data(data, "REACTION_ADD")

        # Fire raw event (always fires, even if message not cached)
        await self._fire("on_raw_reaction_add", raw)

        # Search through channels for the message.
        # Message caching is not implemented yet.
        for channel in self._channels.values():
            # Placeholder for future message cache lookup.
            pass

        # For now, we'll just fire the raw event
        # In a complete implementation, you would:
        # 1. Check message cache
        # 2. Update message reactions
        # 3. Fire on_reaction_add with the full Reaction object

    async def _handle_reaction_remove(self, data: dict[str, Any]) -> None:
        """Handle MESSAGE_REACTION_REMOVE event."""
        from .models.reaction import RawReactionActionEvent

        raw = RawReactionActionEvent.from_data(data, "REACTION_REMOVE")

        # Fire raw event (always fires, even if message not cached)
        await self._fire("on_raw_reaction_remove", raw)

    async def _handle_reaction_remove_all(self, data: dict[str, Any]) -> None:
        """Handle MESSAGE_REACTION_REMOVE_ALL event."""
        from .models.reaction import RawReactionClearEvent

        raw = RawReactionClearEvent.from_data(data)

        # Fire raw event (always fires, even if message not cached)
        await self._fire("on_raw_reaction_clear", raw)

    async def _handle_reaction_remove_emoji(self, data: dict[str, Any]) -> None:
        """Handle MESSAGE_REACTION_REMOVE_EMOJI event."""
        from .models.reaction import RawReactionClearEmojiEvent

        raw = RawReactionClearEmojiEvent.from_data(data)

        # Fire raw event (always fires, even if message not cached)
        await self._fire("on_raw_reaction_clear_emoji", raw)

    async def _fire(self, event_name: str, *args: Any) -> None:
        """Fire all registered handlers for an event."""
        self._dispatch_waiters(event_name, *args)
        handlers = self._event_handlers.get(event_name, [])
        for handler in handlers:
            try:
                await handler(*args)
            except Exception:
                log.exception("Error in event handler '%s'", event_name)

    def _dispatch_waiters(self, event_name: str, *args: Any) -> None:
        waiter_name = event_name[3:] if event_name.startswith("on_") else event_name
        waiters = self._waiters.get(waiter_name)
        if not waiters:
            return

        result = args[0] if len(args) == 1 else args
        remaining = []
        for future, check in waiters:
            if future.cancelled() or future.done():
                continue
            try:
                passed = check is None or check(*args)
            except Exception as exc:
                future.set_exception(exc)
                continue
            if passed:
                future.set_result(result)
            else:
                remaining.append((future, check))

        if remaining:
            self._waiters[waiter_name] = remaining
        else:
            self._waiters.pop(waiter_name, None)

    # =========================================================================
    # HTTP convenience methods
    # =========================================================================

    async def fetch_channel(self, channel_id: str) -> Channel:
        """Fetch a channel from the API (not cache)."""
        assert self._http is not None
        data = await self._http.get_channel(channel_id)
        ch = Channel.from_data(data, self._http)
        self._state.store_channel(ch)
        return ch

    async def fetch_message(self, channel_id: str, message_id: str) -> Message:
        """Fetch a message from the API by channel ID and message ID."""
        assert self._http is not None
        data = await self._http.get_message(channel_id, message_id)
        return self._parse_message(data)

    async def search_messages(
        self,
        *,
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
    ) -> SearchResponse:
        """Search messages in one guild or channel using this bot client.

        Bots can only use Fluxer's ``current`` scope. A guild context takes
        precedence if both context IDs are supplied.
        """
        if context_channel_id is None and context_guild_id is None:
            raise ValueError("A context_channel_id or context_guild_id is required")
        assert self._http is not None
        data = await self._http.search_messages(
            scope="current",
            context_channel_id=context_channel_id,
            context_guild_id=context_guild_id,
            channel_ids=channel_ids,
            channel_id=channel_id,
            hits_per_page=hits_per_page,
            page=page,
            cursor=cursor,
            min_id=min_id,
            max_id=max_id,
            content=content,
            contents=contents,
            exact_phrases=exact_phrases,
            exclude_channel_id=exclude_channel_id,
            author_id=author_id,
            exclude_author_id=exclude_author_id,
            author_type=author_type,
            exclude_author_type=exclude_author_type,
            mentions=mentions,
            exclude_mentions=exclude_mentions,
            mention_everyone=mention_everyone,
            pinned=pinned,
            has=has,
            exclude_has=exclude_has,
            embed_type=embed_type,
            exclude_embed_type=exclude_embed_type,
            embed_provider=embed_provider,
            exclude_embed_provider=exclude_embed_provider,
            link_hostname=link_hostname,
            exclude_link_hostname=exclude_link_hostname,
            attachment_filename=attachment_filename,
            exclude_attachment_filename=exclude_attachment_filename,
            attachment_extension=attachment_extension,
            exclude_attachment_extension=exclude_attachment_extension,
            sort_by=sort_by,
            sort_order=sort_order,
            include_nsfw=include_nsfw,
        )
        return parse_search_response(data, self._http)

    async def delete_message(
        self, channel_id: int | str, message_id: int | str
    ) -> None:
        """Delete a message by channel ID and message ID without fetching it first.

        Args:
            channel_id: The channel ID where the message is located.
            message_id: The message ID to delete.

        Example:
            await client.delete_message(channel_id=123456, message_id=789012)
        """
        assert self._http is not None
        await self._http.delete_message(channel_id, message_id)

    async def fetch_guild(self, guild_id: str) -> Guild:
        """Fetch a guild from the API."""
        assert self._http is not None
        data = await self._http.get_guild(guild_id)
        guild = Guild.from_data(data, self._http)
        self._state.store_guild(guild)
        return guild

    async def fetch_user(self, user_id: str) -> User:
        """Fetch a user from the API."""
        assert self._http is not None
        data = await self._http.get_user(user_id)
        return User.from_data(data, self._http)

    async def fetch_user_profile(
        self, user_id: str, *, guild_id: str | None = None
    ) -> UserProfile:
        """Fetch a user's full profile from the API.

        This returns additional profile information like bio, pronouns, banner, etc.
        that is not included in the basic user object.

        Args:
            user_id: The user ID to fetch
            guild_id: Optional guild ID for guild-specific profile data

        Returns:
            UserProfile containing the user and their profile information
        """
        assert self._http is not None
        data = await self._http.get_user_profile(user_id, guild_id=guild_id)
        return UserProfile.from_data(data, self._http)

    async def fetch_webhook(self, webhook_id: str) -> Webhook:
        """Fetch a webhook from the API."""
        assert self._http is not None
        data = await self._http.get_webhook(webhook_id)
        return Webhook.from_data(data, self._http)

    async def fetch_channel_webhooks(self, channel_id: str) -> list[Webhook]:
        """Fetch all webhooks for a channel."""
        assert self._http is not None
        data = await self._http.get_channel_webhooks(channel_id)
        return [Webhook.from_data(w, self._http) for w in data]

    async def fetch_guild_webhooks(self, guild_id: str) -> list[Webhook]:
        """Fetch all webhooks for a guild."""
        assert self._http is not None
        data = await self._http.get_guild_webhooks(guild_id)
        return [Webhook.from_data(w, self._http) for w in data]

    async def create_webhook(
        self, channel_id: str, *, name: str, avatar: str | None = None
    ) -> Webhook:
        """Create a webhook in a channel."""
        assert self._http is not None
        data = await self._http.create_webhook(channel_id, name=name, avatar=avatar)
        return Webhook.from_data(data, self._http)

    # =========================================================================
    # Voice methods
    # =========================================================================

    async def join_voice(
        self,
        guild_id: int,
        channel_id: int,
        *,
        self_mute: bool = False,
        self_deaf: bool = False,
    ) -> VoiceClient:
        """Join a voice channel and return a connected VoiceClient.

        Requires pip install fluxer.py[voice].
        """
        from .voice import VoiceClient

        if self._gateway is None:
            raise RuntimeError("Cannot join voice before connecting")

        vc = VoiceClient(guild_id, channel_id, self._gateway)
        self._pending_voice[guild_id] = vc
        await self._gateway.update_voice_state(
            guild_id=str(guild_id),
            channel_id=str(channel_id),
            self_mute=self_mute,
            self_deaf=self_deaf,
        )
        await vc._wait_until_connected()
        return vc

    def get_voice_state(self, guild_id: int, user_id: int) -> VoiceState | None:
        """Return the cached voice state for a user in a guild, or None."""
        guild_states = self._voice_states.get(int(guild_id), {})
        for (cached_user_id, _connection_id), state in guild_states.items():
            if cached_user_id == int(user_id):
                return state
        return None

    def get_guild_voice_states(self, guild_id: int) -> list[VoiceState]:
        """Return all cached voice states for a guild."""
        return list(self._voice_states.get(int(guild_id), {}).values())

    def get_channel_voice_states(self, channel_id: int | str) -> list[VoiceState]:
        """Return all cached voice states for a voice channel."""
        target_channel_id = int(channel_id)
        return [
            state
            for guild_states in self._voice_states.values()
            for state in guild_states.values()
            if state.channel_id == target_channel_id
        ]

    def get_channel_voice_user_count(self, channel_id: int | str) -> int:
        """Return the cached unique-user count for a voice channel."""
        return len(
            {state.user_id for state in self.get_channel_voice_states(channel_id)}
        )

    def _voice_state_key(self, voice_state: VoiceState) -> tuple[int, str | None]:
        return (voice_state.user_id, voice_state.connection_id)

    def _seed_guild_voice_states(
        self, guild_id: int, voice_states: list[dict[str, Any]]
    ) -> None:
        for voice_state_data in voice_states:
            payload = {
                **voice_state_data,
                "guild_id": voice_state_data.get("guild_id", guild_id),
            }
            self._store_voice_state(payload)

    def _store_voice_state(self, data: dict[str, Any]) -> VoiceState:
        voice_state = VoiceState.from_data(data, self._http)
        if voice_state.guild_id is not None:
            guild_states = self._voice_states.setdefault(voice_state.guild_id, {})
            key = self._voice_state_key(voice_state)
            if voice_state.channel_id is None:
                guild_states.pop(key, None)
            else:
                guild_states[key] = voice_state
        return voice_state

    # =========================================================================
    # Reaction methods
    # =========================================================================

    async def add_reaction(
        self, channel_id: int | str, message_id: int | str, emoji: str
    ) -> None:
        """Add a reaction to a message by channel_id and message_id.

        Args:
            channel_id: The channel ID
            message_id: The message ID
            emoji: The emoji to react with (unicode string or custom emoji format)

        Raises:
            Forbidden: You don't have permission to add reactions
            NotFound: The message doesn't exist
            HTTPException: Adding the reaction failed
        """
        assert self._http is not None
        await self._http.add_reaction(channel_id, message_id, emoji)

    async def remove_reaction(
        self,
        channel_id: int | str,
        message_id: int | str,
        emoji: str,
        user_id: int | str = "@me",
    ) -> None:
        """Remove a reaction from a message by channel_id and message_id.

        Args:
            channel_id: The channel ID
            message_id: The message ID
            emoji: The emoji to remove (unicode string or custom emoji format)
            user_id: The user ID to remove the reaction from (default: @me)

        Raises:
            Forbidden: You don't have permission to remove this reaction
            NotFound: The message or reaction doesn't exist
            HTTPException: Removing the reaction failed
        """
        assert self._http is not None
        await self._http.delete_reaction(channel_id, message_id, emoji, user_id)

    async def clear_reactions(
        self, channel_id: int | str, message_id: int | str
    ) -> None:
        """Remove all reactions from a message.

        Args:
            channel_id: The channel ID
            message_id: The message ID

        Raises:
            Forbidden: You don't have permission to clear reactions
            NotFound: The message doesn't exist
            HTTPException: Clearing reactions failed
        """
        assert self._http is not None
        await self._http.delete_all_reactions(channel_id, message_id)

    async def clear_reaction(
        self, channel_id: int | str, message_id: int | str, emoji: str
    ) -> None:
        """Remove all reactions of a specific emoji from a message.

        Args:
            channel_id: The channel ID
            message_id: The message ID
            emoji: The emoji to clear (unicode string or custom emoji format)

        Raises:
            Forbidden: You don't have permission to clear reactions
            NotFound: The message doesn't exist
            HTTPException: Clearing reactions failed
        """
        assert self._http is not None
        await self._http.delete_all_reactions_for_emoji(channel_id, message_id, emoji)

    def _require_gateway(self) -> Gateway:
        if self._gateway is None or not self._gateway.is_connected:
            raise GatewayNotConnected("Gateway is not connected")
        return self._gateway

    async def change_presence(
        self,
        *,
        status: str = "online",
        activity: BaseActivity | dict[str, Any] | str | None = None,
        afk: bool = False,
        since: float | None = None,
    ) -> None:
        """Update this client's gateway presence."""
        await self._require_gateway().update_presence(
            status=status,
            activity=activity,
            afk=afk,
            since=since,
        )

    async def request_guild_members(
        self,
        guild_id: int | str,
        *,
        query: str = "",
        limit: int = 0,
        presences: bool = False,
        user_ids: list[int | str] | None = None,
        nonce: str | None = None,
    ) -> None:
        """Request guild members through the Fluxer Gateway."""
        await self._require_gateway().request_guild_members(
            guild_id=guild_id,
            query=query,
            limit=limit,
            presences=presences,
            user_ids=user_ids,
            nonce=nonce,
        )

    async def request_lazy_members(
        self,
        guild_id: int | str,
        *,
        ranges: list[list[int]],
        channels: dict[str, Any] | None = None,
    ) -> None:
        """Request a lazy member-list range through the Fluxer Gateway."""
        await self._require_gateway().request_lazy_members(
            guild_id=guild_id,
            ranges=ranges,
            channels=channels,
        )

    async def request_guild_counts(self, guild_ids: list[int | str]) -> None:
        """Request member/guild statistics through the Fluxer Gateway."""
        await self._require_gateway().request_guild_counts(guild_ids)

    async def request_channel_member_counts(self, channel_ids: list[int | str]) -> None:
        """Request channel member metrics through the Fluxer Gateway."""
        await self._require_gateway().request_channel_member_counts(channel_ids)

    async def setup_hook(self) -> None:
        """Called before connecting to the gateway.

        Override this to perform setup tasks before the client starts receiving events.
        """

    # =========================================================================
    # Connection lifecycle
    # =========================================================================

    async def start(self, token: str) -> None:
        """Connect to Fluxer and start receiving events (async version).

        Use this if you're managing your own event loop.
        """
        self._closed = False
        self._ready.clear()

        # Create HTTP client with custom API URL if provided
        if self.api_url:
            self._http = HTTPClient(
                token,
                api_url=self.api_url,
                max_retries=self._max_retries,
                retry_forever=self._retry_forever,
            )
        else:
            self._http = HTTPClient(
                token, max_retries=self._max_retries, retry_forever=self._retry_forever
            )

        self._state.http = self._http

        self._gateway = Gateway(
            http_client=self._http,
            token=token,
            intents=self.intents,
            dispatch=self._dispatch,
        )

        await self.setup_hook()

        try:
            await self._gateway.connect()
        finally:
            await self.close()

    async def close(self) -> None:
        """Disconnect from the gateway and clean up resources."""
        self._closed = True
        self._ready.clear()
        for waiters in self._waiters.values():
            for future, _check in waiters:
                future.cancel()
        self._waiters.clear()
        if self._gateway:
            await self._gateway.close()
        if self._http:
            await self._http.close()

    def run(self, token: str) -> None:
        """Blocking call that connects to Fluxer and runs the bot.

        This is the simplest way to start your bot:
            bot.run("your_token_here")

        It creates an event loop, calls start(), and handles cleanup.
        """

        async def _runner() -> None:
            try:
                await self.start(token)
            except KeyboardInterrupt:
                pass
            finally:
                if not self._closed:
                    await self.close()

        try:
            asyncio.run(_runner())
        except KeyboardInterrupt:
            log.info("Bot stopped by KeyboardInterrupt")


BotT = TypeVar("BotT", bound="Bot", covariant=True)
Prefix = str | Iterable[str]
PrefixCallable = Callable[[BotT, Message], Prefix | Awaitable[Prefix]]
PrefixType = Prefix | PrefixCallable


class Bot(Client):
    """Extended Client with common bot conveniences.

    Adds prefix command support, cog support, and other bot-specific features.
    This is the recommended class for most bot use cases.

    Args:
        command_prefix: Prefix for text commands (default: "!")
        intents: Gateway intents to request (default: Intents.default())
        api_url: Base URL for the Fluxer API (default: https://api.fluxer.app/v1)
                 Use this to connect to self-hosted Fluxer instances
        max_retries: Maximum number of retries for HTTP requests (default: 4)
        retry_forever: Whether to retry HTTP requests indefinitely (default: False)
    """

    def __init__(
        self,
        *,
        command_prefix: PrefixType = "!",
        intents: Intents = Intents.default(),
        api_url: str | None = None,
        max_retries: int = 4,
        retry_forever: bool = False,
    ) -> None:
        super().__init__(
            intents=intents,
            api_url=api_url,
            max_retries=max_retries,
            retry_forever=retry_forever,
        )
        self.command_prefix = command_prefix
        self._commands: dict[str, EventHandler] = {}
        self._cogs: dict[str, Any] = {}  # Store loaded cogs
        self._extensions: dict[str, Any] = {}  # Store loaded extensions

        # Auto-register the command dispatcher
        @self.event
        async def on_message(message: Message) -> None:
            await self._process_commands(message)

    def command(
        self, name: str | None = None
    ) -> Callable[[EventHandler], EventHandler]:
        """Decorator to register a prefix command.

        Usage:
            @bot.command()
            async def ping(ctx):
                await ctx.reply("Pong!")

            @bot.command(name="hello")
            async def greet(ctx):
                await ctx.reply(f"Hello, {ctx.author}!")
        """

        def decorator(func: EventHandler) -> EventHandler:
            cmd_name = name or func.__name__
            self._commands[cmd_name] = func
            self._commands = dict(
                sorted(self._commands.items(), key=lambda kv: len(kv[0]), reverse=True)
            )  # sorts the dictionary in reverse key length order
            return func

        return decorator

    async def get_prefix(self, message: Message) -> Prefix:
        if callable(self.command_prefix):
            prefix = self.command_prefix(self, message)
            if inspect.isawaitable(prefix):
                prefix = await prefix
            return prefix

        return self.command_prefix

    async def _check_prefix(self, message: Message) -> str | None:
        prefix = await self.get_prefix(message)

        if isinstance(prefix, str):
            return prefix if message.content.startswith(prefix) else None

        if isinstance(prefix, Iterable):
            for p in prefix:
                if message.content.startswith(p):
                    return p

        return None

    async def _process_commands(self, message: Message) -> None:
        """Check if a message matches a registered command and invoke it."""
        if message.author.bot:
            return

        command_prefix = await self._check_prefix(message)
        if not command_prefix:
            return

        # Parse command name and args
        content = message.content[len(command_prefix) :]
        # Use list() to avoid RuntimeError if commands dict is modified during iteration
        for cmd, handler in list(self._commands.items()):
            if content.startswith(cmd):
                if handler:
                    try:
                        # Parse arguments based on function signature
                        args_str = content[len(cmd) :].strip()
                        await self._invoke_command(handler, message, args_str)
                    except TypeError as e:
                        # Handle missing required arguments
                        if "missing" in str(e) and "required" in str(e):
                            await message.reply(f"❌ Error: {e}")
                        else:
                            raise
                    except Exception:
                        log.exception("Error in command '%s'", cmd)
                    break

    async def _invoke_command(
        self, handler: EventHandler, message: Message, args_str: str
    ) -> None:
        """Parse arguments and invoke a command handler.

        Supports:
        - Positional arguments: async def cmd(ctx, arg1, arg2)
        - Keyword-only arguments: async def cmd(ctx, *, message)
        - Type hints: async def cmd(ctx, count: int)
        - Default values: async def cmd(ctx, message: str = "default")
        """
        sig = inspect.signature(handler)
        params = list(sig.parameters.values())

        # First parameter is always the ctx (context/message)
        if not params or params[0].name != "ctx":
            # If function doesn't take ctx as first param, just pass message
            await handler(message)
            return

        # Remove the ctx parameter from processing
        params = params[1:]

        # Check if there are any parameters that need parsing
        if not params:
            await handler(message)
            return

        # Check for keyword-only parameters (indicated by * in signature)
        # e.g., async def say(ctx, *, message: str)
        has_kwonly = any(p.kind == inspect.Parameter.KEYWORD_ONLY for p in params)

        if has_kwonly and len(params) == 1:
            # Single keyword-only argument captures all remaining text
            param = params[0]

            # Check if argument was provided
            if not args_str and param.default == inspect.Parameter.empty:
                raise TypeError(
                    f"{handler.__name__}() missing 1 required keyword-only argument: '{param.name}'"
                )

            # Use default if no args provided
            if not args_str:
                await handler(message)
                return

            # Convert to the appropriate type if type hint exists
            value = self._convert_argument(args_str, param.annotation)
            await handler(message, **{param.name: value})
        else:
            # Multiple positional or mixed arguments
            # Split args_str into individual arguments
            args = args_str.split() if args_str else []

            # Build the argument list
            call_args = [message]
            call_kwargs = {}

            for i, param in enumerate(params):
                if param.kind == inspect.Parameter.KEYWORD_ONLY:
                    # Keyword-only args capture remaining text
                    remaining = " ".join(args[i:]) if i < len(args) else ""
                    if not remaining and param.default == inspect.Parameter.empty:
                        raise TypeError(
                            f"{handler.__name__}() missing 1 required keyword-only argument: '{param.name}'"
                        )
                    if remaining:
                        value = self._convert_argument(remaining, param.annotation)
                        call_kwargs[param.name] = value
                    break
                else:
                    # Positional argument
                    if i < len(args):
                        value = self._convert_argument(args[i], param.annotation)
                        call_args.append(value)
                    elif param.default != inspect.Parameter.empty:
                        # Use default value
                        break
                    else:
                        raise TypeError(
                            f"{handler.__name__}() missing 1 required positional argument: '{param.name}'"
                        )

            await handler(*call_args, **call_kwargs)

    def _convert_argument(self, value: str, annotation: Any) -> Any:
        """Convert a string argument to the appropriate type based on annotation."""
        if annotation == inspect.Parameter.empty or annotation is str:
            return value

        try:
            if annotation is int:
                return int(value)
            elif annotation is float:
                return float(value)
            elif annotation is bool:
                return value.lower() in ("true", "1", "yes", "y")
            else:
                # Try to call the annotation as a constructor
                return annotation(value)
        except (ValueError, TypeError):
            # If conversion fails, return as string
            return value

    # =========================================================================
    # Cog management
    # =========================================================================

    async def add_cog(self, cog: Any) -> None:
        """Add a cog to the bot.

        Args:
            cog: An instance of a Cog subclass.

        Example:
            class MyCog(Cog):
                @Cog.command()
                async def hello(self, ctx):
                    await ctx.reply("Hello!")

            bot = Bot()
            await bot.add_cog(MyCog(bot))
        """
        cog_name = cog.__class__.__name__

        if cog_name in self._cogs:
            raise ValueError(f"Cog '{cog_name}' is already loaded")

        # Register cog's commands
        for cmd_name, handler in cog._commands.items():
            if cmd_name in self._commands:
                log.warning(
                    "Command '%s' from cog '%s' overwrites existing command",
                    cmd_name,
                    cog_name,
                )
            self._commands[cmd_name] = handler

        self._commands = dict(
            sorted(self._commands.items(), key=lambda kv: len(kv[0]), reverse=True)
        )  # sorts the dictionary in reverse key length order

        # Register cog's event listeners
        for event_name, listeners in cog._listeners.items():
            for listener in listeners:
                # Add to the client's event handlers
                if event_name not in self._event_handlers:
                    self._event_handlers[event_name] = []
                self._event_handlers[event_name].append(listener)

        # Store the cog
        self._cogs[cog_name] = cog

        # Call the cog's load hook
        await cog.cog_load()

        log.info("Loaded cog: %s", cog_name)

    async def remove_cog(self, cog_name: str) -> None:
        """Remove a cog from the bot.

        Args:
            cog_name: The name of the cog class to remove.

        Example:
            await bot.remove_cog("MyCog")
        """
        if cog_name not in self._cogs:
            raise ValueError(f"Cog '{cog_name}' is not loaded")

        cog = self._cogs[cog_name]

        # Call the cog's unload hook
        await cog.cog_unload()

        # Remove cog's commands
        for cmd_name in list(cog._commands.keys()):
            self._commands.pop(cmd_name, None)

        # Remove cog's event listeners
        for event_name, listeners in cog._listeners.items():
            if event_name in self._event_handlers:
                for listener in listeners:
                    try:
                        self._event_handlers[event_name].remove(listener)
                    except ValueError:
                        pass

        # Remove the cog
        del self._cogs[cog_name]

        log.info("Removed cog: %s", cog_name)

    async def reload_cog(self, cog_name: str) -> None:
        """Reload a cog by removing and re-adding it.

        This is useful during development to reload code changes without restarting the bot.
        Note: You'll need to reimport the module and create a new instance.

        Args:
            cog_name: The name of the cog class to reload.

        Example:
            import importlib
            import my_cogs

            # Reload the module
            importlib.reload(my_cogs)

            # Remove old cog
            await bot.remove_cog("MyCog")

            # Add new cog
            await bot.add_cog(my_cogs.MyCog(bot))
        """
        if cog_name not in self._cogs:
            raise ValueError(f"Cog '{cog_name}' is not loaded")

        # For simple reload, just remove and let the user re-add
        await self.remove_cog(cog_name)
        log.info("Cog '%s' removed. Please re-add it with add_cog().", cog_name)

    def get_cog(self, cog_name: str) -> Any | None:
        """Get a loaded cog by name.

        Args:
            cog_name: The name of the cog class.

        Returns:
            The cog instance, or None if not found.
        """
        return self._cogs.get(cog_name)

    @property
    def cogs(self) -> dict[str, Any]:
        """Get all loaded cogs."""
        return self._cogs.copy()

    # =========================================================================
    # Extension management
    # =========================================================================

    async def load_extension(self, name: str) -> None:
        """Load an extension (module) containing cogs and commands.

        The extension must have
        a setup() function that takes the bot instance as an argument.

        Args:
            name: The module path (e.g., "cogs.moderation" or "my_cogs.fun")

        Example:
            # In cogs/moderation.py:
            from fluxer import Cog

            class ModerationCog(Cog):
                @Cog.command()
                async def ban(self, message):
                    await message.reply("Ban command!")

            async def setup(bot):
                await bot.add_cog(ModerationCog(bot))

            # In your main bot file:
            await bot.load_extension("cogs.moderation")
        """
        if name in self._extensions:
            raise ValueError(f"Extension '{name}' is already loaded")

        # Import the module
        try:
            module = importlib.import_module(name)
        except ImportError as e:
            raise ImportError(f"Failed to import extension '{name}': {e}") from e

        # Check if module has a setup function
        if not hasattr(module, "setup"):
            raise AttributeError(
                f"Extension '{name}' is missing a setup() function. "
                "Extensions must have an async setup(bot) function."
            )

        setup = module.setup

        # Call the setup function
        try:
            if inspect.iscoroutinefunction(setup):
                await setup(self)
            else:
                setup(self)
        except Exception as e:
            raise RuntimeError(f"Failed to load extension '{name}': {e}") from e

        # Store the module
        self._extensions[name] = module
        log.info("Loaded extension: %s", name)

    async def unload_extension(self, name: str) -> None:
        """Unload an extension.

        Args:
            name: The module path of the extension to unload.

        Example:
            await bot.unload_extension("cogs.moderation")
        """
        if name not in self._extensions:
            raise ValueError(f"Extension '{name}' is not loaded")

        module = self._extensions[name]

        # Call teardown function if it exists
        if hasattr(module, "teardown"):
            teardown = module.teardown
            try:
                if inspect.iscoroutinefunction(teardown):
                    await teardown(self)
                else:
                    teardown(self)
            except Exception:
                log.exception("Error in teardown for extension '%s'", name)

        # Remove from sys.modules to allow fresh reload
        if name in sys.modules:
            del sys.modules[name]

        # Remove from extensions dict
        del self._extensions[name]
        log.info("Unloaded extension: %s", name)

    async def reload_extension(self, name: str) -> None:
        """Reload an extension by unloading and loading it again.

        This is useful during development to reload code changes without restarting.

        Args:
            name: The module path of the extension to reload.

        Example:
            await bot.reload_extension("cogs.moderation")
        """
        if name not in self._extensions:
            raise ValueError(f"Extension '{name}' is not loaded")

        # Unload and reload
        await self.unload_extension(name)
        await self.load_extension(name)
        log.info("Reloaded extension: %s", name)

    @property
    def extensions(self) -> dict[str, Any]:
        """Get all loaded extensions."""
        return self._extensions.copy()


def when_mentioned(bot: Bot, message: Message, /) -> list[str]:
    """A callable that returns the bot's mention as a prefix.

    Intended for use with command_prefix

        bot = Bot(command_prefix=when_mentioned)

    Returns:
        A list containing the bot's mention string.
    """
    return [f"<@{bot.user.id}> "]  # type: ignore


def when_mentioned_or(*prefixes: str) -> Callable[[Bot, Message], list[str]]:
    """A callable that returns the bot's mention and the provided prefixes.

    This is a convenience function that combines when_mentioned
    with custom prefixes

        bot = Bot(command_prefix=when_mentioned_or("!", "?"))

    Args:
        *prefixes: Additional prefixes the bot should respond to.

    Returns:
        A callable suitable for command_prefix.
    """

    def inner(bot: Bot, message: Message) -> list[str]:
        return when_mentioned(bot, message) + list(prefixes)

    return inner
