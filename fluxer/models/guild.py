from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from fluxer.models.emoji import Emoji
from fluxer.models.member import GuildMember
from fluxer.models.role import Role
from fluxer.sticker import Sticker

from ..utils import snowflake_to_datetime
from ..fluxer_models import (
    SearchAuthorType,
    SearchContentType,
    SearchEmbedType,
    SearchResponse,
    SearchSortBy,
    SearchSortOrder,
    parse_search_response,
)

if TYPE_CHECKING:
    from ..http import HTTPClient


@dataclass(slots=True)
class Guild:
    """Represents a Fluxer guild (server/community)."""

    id: int
    name: str | None = None
    icon: str | None = None
    owner_id: int | None = None
    member_count: int | None = None
    unavailable: bool = False
    roles: list[Role] = field(default_factory=list)

    _http: HTTPClient | None = field(default=None, repr=False)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: HTTPClient | None = None) -> Guild:
        return cls(
            id=int(data["id"]),
            name=data.get("name"),
            icon=data.get("icon"),
            owner_id=int(data["owner_id"]) if data.get("owner_id") else None,
            member_count=data.get("member_count"),
            unavailable=data.get("unavailable", False),
            _http=http,
        )

    @property
    def created_at(self) -> datetime:
        return snowflake_to_datetime(self.id)

    @property
    def icon_url(self) -> str | None:
        if self.icon:
            ext = "gif" if self.icon.startswith("a_") else "png"
            return f"https://fluxerusercontent.com/icons/{self.id}/{self.icon}.{ext}"
        return None

    async def fetch_emojis(self) -> list[Emoji]:
        """Fetch all emojis in this guild.

        Returns:
            List of Emoji objects
        """
        if not self._http:
            raise RuntimeError("Cannot fetch emojis without HTTPClient")

        from .emoji import Emoji

        data = await self._http.get_guild_emojis(self.id)
        # Pass guild_id when creating emojis since API doesn't always return it
        return [
            Emoji.from_data(emoji_data, self._http, guild_id=self.id)
            for emoji_data in data
        ]

    # -- Role Management Methods --
    async def fetch_roles(self) -> list[Role]:
        """Fetch all roles in this guild.

        Returns:
            List of Role objects
        """
        if not self._http:
            raise RuntimeError("Cannot fetch roles without HTTPClient")

        from .role import Role

        data = await self._http.get_guild_roles(self.id)
        return [
            Role.from_data(role_data, self._http, guild_id=self.id)
            for role_data in data
        ]

    async def create_role(
        self,
        *,
        name: str | None = None,
        permissions: int | None = None,
        color: int = 0,
        hoist: bool = False,
        mentionable: bool = False,
    ) -> Role:
        """Create a new role in this guild.

        Args:
            name: Role name
            permissions: Permission bitfield
            color: Role color
            hoist: Whether to display role separately
            mentionable: Whether role can be mentioned

        Returns:
            Role object
        """
        if not self._http:
            raise RuntimeError("Cannot create role without HTTPClient")

        from .role import Role

        data = await self._http.create_guild_role(
            self.id,
            name=name,
            permissions=permissions,
            color=color,
            hoist=hoist,
            mentionable=mentionable,
        )
        return Role.from_data(data, self._http, guild_id=self.id)

    # -- Member Management Methods --
    async def fetch_member(self, user_id: int) -> GuildMember:
        """Fetch a specific member from this guild.

        Args:
            user_id: User ID to fetch

        Returns:
            GuildMember object
        """
        if not self._http:
            raise RuntimeError("Cannot fetch member without HTTPClient")

        from .member import GuildMember

        data = await self._http.get_guild_member(self.id, user_id)
        return GuildMember.from_data(data, self._http, guild_id=self.id)

    async def fetch_members(
        self, *, limit: int = 100, after: int | None = None
    ) -> list[GuildMember]:
        """Fetch members from this guild.

        Args:
            limit: Maximum number of members to fetch (1-1000)
            after: Fetch members after this user ID

        Returns:
            List of GuildMember objects
        """
        if not self._http:
            raise RuntimeError("Cannot fetch members without HTTPClient")

        from .member import GuildMember

        data = await self._http.get_guild_members(self.id, limit=limit, after=after)
        return [
            GuildMember.from_data(member_data, self._http, guild_id=self.id)
            for member_data in data
        ]

    # -- Moderation Methods --
    async def kick(self, user_id: int, *, reason: str | None = None) -> None:
        """Kick a member from this guild.

        Args:
            user_id: User ID to kick
            reason: Reason for audit log
        """
        if not self._http:
            raise RuntimeError("Cannot kick member without HTTPClient")

        await self._http.kick_guild_member(self.id, user_id, reason=reason)

    async def ban(
        self,
        user_id: int,
        *,
        ban_duration_seconds: int = 0,
        delete_message_days: int = 0,
        delete_message_seconds: int = 0,
        reason: str | None = None,
    ) -> None:
        """Ban a user from this guild.

        Args:
            user_id: User ID to ban
            ban_duration_seconds: Duration of the ban in seconds (0 for permanent, or a valid temporary duration)
            delete_message_days: Number of days to delete messages for (0-7)
            delete_message_seconds: Number of seconds to delete messages for (0-604800)
            reason: Reason for audit log
        """
        if not self._http:
            raise RuntimeError("Cannot ban member without HTTPClient")

        await self._http.ban_guild_member(
            self.id,
            user_id,
            ban_duration_seconds=ban_duration_seconds,
            delete_message_days=delete_message_days,
            delete_message_seconds=delete_message_seconds,
            reason=reason,
        )

    async def unban(self, user_id: int, *, reason: str | None = None) -> None:
        """Unban a user from this guild.

        Args:
            user_id: User ID to unban
            reason: Reason for audit log
        """
        if not self._http:
            raise RuntimeError("Cannot unban user without HTTPClient")

        await self._http.unban_guild_member(self.id, user_id, reason=reason)

    async def invites(self) -> list[Any]:
        """Fetch invites for this guild."""
        from ..invite import Invite

        if not self._http:
            raise RuntimeError("Cannot fetch invites without HTTPClient")
        data = await self._http.get_guild_invites(self.id)
        return [Invite.from_data(item, self._http) for item in data]

    async def audit_logs(self, **kwargs: Any) -> Any:
        """Fetch this guild's audit log."""
        from ..audit_logs import AuditLog

        if not self._http:
            raise RuntimeError("Cannot fetch audit logs without HTTPClient")
        data = await self._http.get_guild_audit_logs(self.id, **kwargs)
        return AuditLog.from_data(data)

    async def fetch_stickers(self) -> list[Sticker]:
        """Fetch all stickers in this guild."""
        if not self._http:
            raise RuntimeError("Cannot fetch stickers without HTTPClient")
        data = await self._http.get_guild_stickers(self.id)
        return [Sticker.from_data(item, self._http) for item in data]

    async def discovery_status(self) -> Any:
        from ..fluxer_models import DiscoveryStatus

        if not self._http:
            raise RuntimeError("Cannot fetch discovery status without HTTPClient")
        return DiscoveryStatus.from_data(
            await self._http.get_guild_discovery_status(self.id), self._http
        )

    async def apply_for_discovery(self, **payload: Any) -> Any:
        from ..fluxer_models import DiscoveryApplication

        if not self._http:
            raise RuntimeError("Cannot apply for discovery without HTTPClient")
        method = getattr(self._http, "apply_for_guild_discovery", None)
        if method is None:
            method = self._http.apply_for_discovery
        return DiscoveryApplication.from_data(
            await method(self.id, **payload), self._http
        )

    async def edit_discovery_application(self, **payload: Any) -> Any:
        from ..fluxer_models import DiscoveryApplication

        if not self._http:
            raise RuntimeError("Cannot edit discovery application without HTTPClient")
        method = getattr(self._http, "edit_guild_discovery_application", None)
        if method is None:
            method = self._http.edit_discovery_application
        return DiscoveryApplication.from_data(
            await method(self.id, **payload),
            self._http,
        )

    async def withdraw_discovery_application(self) -> None:
        if not self._http:
            raise RuntimeError(
                "Cannot withdraw discovery application without HTTPClient"
            )
        await self._http.withdraw_discovery_application(self.id)

    async def join_discovery(self) -> Any:
        if not self._http:
            raise RuntimeError("Cannot join discovery guild without HTTPClient")
        return await self._http.join_discovery_guild(self.id)

    async def get_vanity_url(self) -> Any:
        from ..fluxer_models import VanityUrl

        if not self._http:
            raise RuntimeError("Cannot fetch vanity URL without HTTPClient")
        return VanityUrl.from_data(
            await self._http.get_guild_vanity_url(self.id), self._http
        )

    async def update_vanity_url(self, code: str) -> Any:
        from ..fluxer_models import VanityUrl

        if not self._http:
            raise RuntimeError("Cannot update vanity URL without HTTPClient")
        return VanityUrl.from_data(
            await self._http.update_guild_vanity_url(self.id, code), self._http
        )

    async def transfer_ownership(self, new_owner_id: int | str, **payload: Any) -> Any:
        from ..fluxer_models import GuildTransferResult

        if not self._http:
            raise RuntimeError("Cannot transfer ownership without HTTPClient")
        return GuildTransferResult.from_data(
            await self._http.transfer_guild_ownership(self.id, new_owner_id, **payload),
            self._http,
        )

    async def bulk_create_emojis(self, emojis: list[dict[str, Any]]) -> Any:
        from ..fluxer_models import BulkEmojiResult

        if not self._http:
            raise RuntimeError("Cannot create emojis without HTTPClient")
        return BulkEmojiResult.from_data(
            await self._http.bulk_create_guild_emojis(self.id, emojis), self._http
        )

    async def bulk_create_stickers(self, stickers: list[dict[str, Any]]) -> Any:
        from ..fluxer_models import BulkStickerResult

        if not self._http:
            raise RuntimeError("Cannot create stickers without HTTPClient")
        return BulkStickerResult.from_data(
            await self._http.bulk_create_guild_stickers(self.id, stickers), self._http
        )

    async def clone_emoji(self, **payload: Any) -> Emoji:
        if not self._http:
            raise RuntimeError("Cannot clone emoji without HTTPClient")
        data = await self._http.clone_guild_emoji(self.id, **payload)
        return Emoji.from_data(data, self._http, guild_id=self.id)

    async def clone_sticker(self, **payload: Any) -> Sticker:
        if not self._http:
            raise RuntimeError("Cannot clone sticker without HTTPClient")
        data = await self._http.clone_guild_sticker(self.id, **payload)
        return Sticker.from_data(data, self._http)

    async def search_messages(
        self,
        *,
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
        """Search messages in this guild using Fluxer's bot-safe current scope."""
        if not self._http:
            raise RuntimeError("Cannot search messages without HTTPClient")
        data = await self._http.search_messages(
            scope="current",
            context_guild_id=self.id,
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

    def __str__(self) -> str:
        return self.name or f"Guild({self.id})"
