from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any

from fluxer.utils import process_embed_args

from ..enums import ChannelType
from ..fluxer_models import (
    SearchAuthorType,
    SearchContentType,
    SearchEmbedType,
    SearchResponse,
    SearchSortBy,
    SearchSortOrder,
    parse_search_response,
)
from ..utils import snowflake_to_datetime

if TYPE_CHECKING:
    from ..client import Client
    from ..file import File
    from ..http import HTTPClient
    from ..voice import VoiceClient
    from .embed import Embed
    from .guild import Guild
    from .message import Message, PartialMessage


class _TypingContext:
    def __init__(self, channel: Channel) -> None:
        self.channel = channel

    def __await__(self):
        return self._send().__await__()

    async def __aenter__(self) -> _TypingContext:
        await self._send()
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def _send(self) -> None:
        await self.channel.trigger_typing()


@dataclass(slots=True)
class Channel:
    """Represents a Fluxer channel (text, DM, voice, category, etc.)."""

    id: int
    type: int
    name: str | None = None
    guild_id: int | None = None
    position: int | None = None
    topic: str | None = None
    nsfw: bool = False
    parent_id: int | None = None

    _http: HTTPClient | None = field(default=None, repr=False)
    _guild: Guild | None = field(default=None, repr=False)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: HTTPClient | None = None) -> Channel:
        return cls(
            id=int(data["id"]),
            type=data["type"],
            name=data.get("name"),
            guild_id=int(data["guild_id"]) if data.get("guild_id") else None,
            position=data.get("position"),
            topic=data.get("topic"),
            nsfw=data.get("nsfw", False),
            parent_id=int(data["parent_id"]) if data.get("parent_id") else None,
            _http=http,
        )

    @property
    def guild(self) -> Guild | None:
        return self._guild

    @property
    def mention(self) -> str:
        return f"<#{self.id}>"

    @property
    def created_at(self) -> datetime:
        return snowflake_to_datetime(self.id)

    @property
    def is_text_channel(self) -> bool:
        """Whether this is a guild text channel."""
        return self.type == ChannelType.GUILD_TEXT

    @property
    def is_voice_channel(self) -> bool:
        """Whether this is a voice channel."""
        return self.type == ChannelType.GUILD_VOICE

    @property
    def is_dm(self) -> bool:
        """Whether this is a DM channel."""
        return self.type == ChannelType.DM

    @property
    def is_category(self) -> bool:
        """Whether this is a category channel."""
        return self.type == ChannelType.GUILD_CATEGORY

    async def send(
        self,
        content: str | None = None,
        *,
        embed: Embed | None = None,
        embeds: list[Embed] | None = None,
        file: File | None = None,
        files: list[File] | None = None,
        message_reference: dict[str, Any] | None = None,
        allowed_mentions: Any | None = None,
        **kwargs: Any,
    ) -> Message:
        """Send a message to this channel.

        Args:
            content: Text content of the message.
            embed: A single embed to include.
            embeds: Multiple embeds to include.
            file: A single File object to attach.
            files: Multiple File objects to attach.
            message_reference: Reference to another message for replies.
            allowed_mentions: Controls which mentions notify users.

        Returns:
            The created Message object.

        Examples:
            # Send a file from path
            from fluxer import File
            await channel.send("Hello!", file=File("image.png"))

            # Send multiple files
            await channel.send("Files:", files=[File("a.txt"), File("b.txt")])

            # Send file with embed
            embed = Embed(title="Title")
            await channel.send(embed=embed, file=File("data.json"))
        """
        # Import here to avoid circular imports
        from .message import Message

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        # Auto-convert single embed to embeds list
        combined_kwargs = {"embed": embed, "embeds": embeds, **kwargs}
        combined_kwargs = process_embed_args(combined_kwargs)

        # Handle file/files parameter - convert File objects to dict format
        file_list: list[dict[str, Any]] | None = None
        if file is not None:
            file_list = [file.to_dict()]
        elif files is not None:
            file_list = [f.to_dict() for f in files]

        data = await self._http.send_message(
            self.id,
            content=content,
            files=file_list,
            message_reference=message_reference,
            allowed_mentions=allowed_mentions,
            **combined_kwargs,
        )
        msg = Message.from_data(data, self._http)
        msg._channel = self
        msg._cache_guild(self._guild)
        return msg

    async def fetch_message(self, message_id: int | str) -> Message:
        """Fetch a message from this channel by ID.

        Args:
            message_id: The message ID to fetch.

        Returns:
            The fetched Message object.
        """
        from .message import Message

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        data = await self._http.get_message(self.id, message_id)
        msg = Message.from_data(data, self._http)
        msg._channel = self
        msg._cache_guild(self._guild)
        return msg

    async def fetch_messages(self, limit: int = 50) -> list[Message]:
        """Fetch recent messages from this channel.

        Args:
            limit: The maximum number of messages to fetch (default 50).

        Returns:
            A list of Message objects.
        """
        from .message import Message

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        data = await self._http.get_messages(self.id, limit=limit)
        msgs = [Message.from_data(msg_data, self._http) for msg_data in data]
        for msg in msgs:
            msg._channel = self
            msg._cache_guild(self._guild)
        return msgs

    def get_partial_message(self, message_id: int | str) -> PartialMessage:
        """Return a lightweight message handle for this channel."""
        from .message import PartialMessage

        return PartialMessage(
            channel_id=self.id,
            id=int(message_id),
            _http=self._http,
            _channel=self,
            _guild=self._guild,
        )

    async def history(
        self,
        *,
        limit: int = 50,
        before: int | str | None = None,
        after: int | str | None = None,
        around: int | str | None = None,
    ) -> AsyncIterator[Message]:
        """Iterate over recent messages in this channel."""
        from .message import Message

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        data = await self._http.get_messages(
            self.id,
            limit=limit,
            before=before,
            after=after,
            around=around,
        )
        for msg_data in data:
            msg = Message.from_data(msg_data, self._http)
            msg._channel = self
            msg._cache_guild(self._guild)
            yield msg

    async def purge(
        self,
        *,
        limit: int = 50,
        check: Callable[[Message], bool] | None = None,
        before: int | str | None = None,
        after: int | str | None = None,
        around: int | str | None = None,
    ) -> list[Message]:
        """Delete recent messages selected by an optional predicate."""
        deleted: list[Message] = []
        async for message in self.history(
            limit=limit,
            before=before,
            after=after,
            around=around,
        ):
            if check is None or check(message):
                deleted.append(message)

        if deleted:
            if self._http is None:
                raise RuntimeError("Channel is not bound to an HTTP client")
            await self._http.delete_messages(
                self.id, [message.id for message in deleted]
            )
        return deleted

    async def fetch_pinned_messages(
        self,
        *,
        limit: int | None = None,
        before: int | str | None = None,
    ) -> list[Message]:
        """Fetch all pinned messages from this channel.

        Returns:
            A list of pinned Message objects.
        """
        from .message import Message

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        data = await self._http.get_pinned_messages(self.id, limit=limit, before=before)
        msgs = [
            Message.from_data(msg_data.get("message", msg_data), self._http)
            for msg_data in data
        ]
        for msg in msgs:
            msg._channel = self
            msg._guild = self._guild
        return msgs

    async def ack_pins(self) -> None:
        """Acknowledge this channel's current pin state."""
        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")
        if hasattr(self._http, "ack_pins"):
            await self._http.ack_pins(self.id)
        else:
            await self._http.acknowledge_pins(self.id)

    async def invites(self) -> list[Any]:
        """Fetch invites for this channel."""
        from ..invite import Invite

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")
        data = await self._http.get_channel_invites(self.id)
        return [Invite.from_data(item, self._http) for item in data]

    async def create_invite(self, **kwargs: Any) -> Any:
        """Create an invite for this channel."""
        from ..invite import Invite

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")
        data = await self._http.create_channel_invite(self.id, **kwargs)
        return Invite.from_data(data, self._http)

    async def delete_messages(self, message_ids: list[int | str]) -> None:
        """Bulk delete messages in this channel.

        Args:
            message_ids: A list of message IDs to delete.

        """
        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        await self._http.delete_messages(self.id, message_ids)

    async def trigger_typing(self) -> None:
        """Trigger a typing indicator in this channel."""

        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")

        return await self._http.trigger_typing(self.id)

    def typing(self) -> _TypingContext:
        """Return a typing indicator helper for this channel."""
        return _TypingContext(self)

    async def search_messages(
        self,
        *,
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
    ) -> SearchResponse:
        """Search messages in this channel using Fluxer's current scope."""
        if self._http is None:
            raise RuntimeError("Channel is not bound to an HTTP client")
        data = await self._http.search_messages(
            scope="current",
            context_channel_id=self.id,
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
        )
        return parse_search_response(data, self._http)

    async def connect(
        self,
        client: Client,
        *,
        self_mute: bool = False,
        self_deaf: bool = False,
    ) -> VoiceClient:
        """Join this voice channel and return a connected VoiceClient.

        Requires fluxer.py[voice].
        """
        if not self.is_voice_channel:
            raise TypeError(f"Cannot connect to a non-voice channel (type={self.type})")
        if self.guild_id is None:
            raise ValueError("Cannot connect to a voice channel without a guild_id")
        return await client.join_voice(
            self.guild_id, self.id, self_mute=self_mute, self_deaf=self_deaf
        )

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Channel) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)
