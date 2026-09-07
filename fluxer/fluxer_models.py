from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

if TYPE_CHECKING:
    from .models import Channel, Message


SearchScope: TypeAlias = Literal[
    "current",
    "open_dms",
    "all_dms",
    "all_guilds",
    "all",
    "open_dms_and_all_guilds",
]
SearchAuthorType: TypeAlias = Literal["user", "bot", "webhook"]
SearchContentType: TypeAlias = Literal[
    "image", "sound", "video", "file", "sticker", "embed", "link", "poll", "snapshot"
]
SearchEmbedType: TypeAlias = Literal["image", "video", "sound", "article"]
SearchSortBy: TypeAlias = Literal["timestamp", "relevance"]
SearchSortOrder: TypeAlias = Literal["asc", "desc"]


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _unwrap(data: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict):
            return value
    return data


@dataclass(slots=True)
class AttachmentUploadSpec:
    """Fluxer attachment upload specification for the presigned upload route."""

    id: int
    filename: str
    file_size: int
    content_type: str

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> AttachmentUploadSpec:
        return cls(
            id=int(data["id"]),
            filename=data["filename"],
            file_size=int(data["file_size"]),
            content_type=data["content_type"],
        )

    def to_dict(self) -> dict[str, Any]:
        """Return this upload specification in Fluxer's request shape."""
        return {
            "id": self.id,
            "filename": self.filename,
            "file_size": self.file_size,
            "content_type": self.content_type,
        }


@dataclass(slots=True)
class AttachmentUploadPart:
    """One presigned multipart upload part returned by Fluxer."""

    part_number: int
    upload_url: str

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> AttachmentUploadPart:
        return cls(part_number=int(data["part_number"]), upload_url=data["upload_url"])


@dataclass(slots=True)
class AttachmentUpload:
    """Presigned upload details for one Fluxer message attachment."""

    id: int
    filename: str
    upload_filename: str
    file_size: int
    content_type: str
    upload_mode: str
    upload_url: str | None = None
    upload_id: str | None = None
    part_size: int | None = None
    parts: list[AttachmentUploadPart] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> AttachmentUpload:
        return cls(
            id=int(data["id"]),
            filename=data["filename"],
            upload_filename=data["upload_filename"],
            file_size=int(data["file_size"]),
            content_type=data["content_type"],
            upload_mode=data["upload_mode"],
            upload_url=data.get("upload_url"),
            upload_id=data.get("upload_id"),
            part_size=_maybe_int(data.get("part_size")),
            parts=[
                AttachmentUploadPart.from_data(part) for part in data.get("parts", [])
            ],
            raw_data=data,
        )

    @property
    def is_multipart(self) -> bool:
        """Return whether this upload requires multipart completion."""
        return self.upload_mode == "multipart"

    def to_attachment_payload(
        self, *, description: str | None = None
    ) -> dict[str, Any]:
        """Return the message ``attachments`` payload item for this upload."""
        payload: dict[str, Any] = {
            "id": self.id,
            "filename": self.filename,
            "uploaded_filename": self.upload_filename,
        }
        if description is not None:
            payload["description"] = description
        return payload


@dataclass(slots=True)
class AttachmentUploadPlan:
    """Response from Fluxer's presigned message attachment upload route."""

    attachments: list[AttachmentUpload] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> AttachmentUploadPlan:
        return cls(
            attachments=[
                AttachmentUpload.from_data(item) for item in data.get("attachments", [])
            ],
            raw_data=data,
        )


@dataclass(slots=True)
class CompletedAttachmentUpload:
    """Finalized Fluxer multipart attachment upload key."""

    upload_filename: str
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> CompletedAttachmentUpload:
        return cls(upload_filename=data["upload_filename"], raw_data=data)


@dataclass(slots=True)
class CompletedAttachmentUploadList:
    """Response from Fluxer's multipart attachment completion route."""

    uploads: list[CompletedAttachmentUpload] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> CompletedAttachmentUploadList:
        return cls(
            uploads=[
                CompletedAttachmentUpload.from_data(item)
                for item in data.get("uploads", [])
            ],
            raw_data=data,
        )


@dataclass(slots=True)
class SavedMessage:
    """A private saved-message entry for the authenticated user."""

    id: int | None = None
    message_id: int | None = None
    channel_id: int | None = None
    guild_id: int | None = None
    status: str | None = None
    saved_at: str | None = None
    message: Any | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> SavedMessage:
        payload = _unwrap(data, "saved_message", "entry")
        message_data = payload.get("message")
        message = None
        if isinstance(message_data, dict):
            from .models import Message

            message = Message.from_data(message_data, http)
        return cls(
            id=_maybe_int(payload.get("id")),
            message_id=_maybe_int(
                payload.get("message_id") or (message_data or {}).get("id")
            ),
            channel_id=_maybe_int(
                payload.get("channel_id") or (message_data or {}).get("channel_id")
            ),
            guild_id=_maybe_int(
                payload.get("guild_id") or (message_data or {}).get("guild_id")
            ),
            status=payload.get("status"),
            saved_at=payload.get("saved_at") or payload.get("created_at"),
            message=message,
            raw_data=data,
        )


@dataclass(slots=True)
class ScheduledMessage:
    """A user-token scheduled message managed by Fluxer."""

    id: str
    channel_id: int | None = None
    scheduled_at: str | None = None
    scheduled_local_at: str | None = None
    timezone: str | None = None
    status: str | None = None
    status_reason: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> ScheduledMessage:
        payload = _unwrap(data, "scheduled_message")
        return cls(
            id=str(payload.get("id") or payload.get("scheduled_message_id")),
            channel_id=_maybe_int(payload.get("channel_id")),
            scheduled_at=payload.get("scheduled_at"),
            scheduled_local_at=payload.get("scheduled_local_at"),
            timezone=payload.get("timezone"),
            status=payload.get("status"),
            status_reason=payload.get("status_reason"),
            payload=payload.get("message") or payload.get("payload") or {},
            raw_data=data,
        )


@dataclass(slots=True)
class Mention:
    """A recent mention entry for the authenticated user."""

    message: Any | None = None
    message_id: int | None = None
    channel_id: int | None = None
    guild_id: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> Mention:
        payload = _unwrap(data, "mention", "message")
        message = None
        if "content" in payload and "author" in payload:
            from .models import Message

            message = Message.from_data(payload, http)
        return cls(
            message=message,
            message_id=_maybe_int(payload.get("id") or payload.get("message_id")),
            channel_id=_maybe_int(payload.get("channel_id")),
            guild_id=_maybe_int(payload.get("guild_id")),
            raw_data=data,
        )


@dataclass(slots=True)
class Relationship:
    """A Fluxer account relationship. User-token sensitive."""

    id: int | None = None
    user_id: int | None = None
    type: str | int | None = None
    nickname: str | None = None
    since: str | None = None
    user: Any | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> Relationship:
        payload = _unwrap(data, "relationship")
        user_data = payload.get("user")
        user = None
        if isinstance(user_data, dict):
            from .models import User

            user = User.from_data(user_data, http)
        return cls(
            id=_maybe_int(payload.get("id")),
            user_id=_maybe_int(payload.get("user_id") or (user_data or {}).get("id")),
            type=payload.get("type"),
            nickname=payload.get("nickname"),
            since=payload.get("since") or payload.get("created_at"),
            user=user,
            raw_data=data,
        )


@dataclass(slots=True)
class FavoriteMeme:
    """A Fluxer saved-media favorite meme entry. User-token sensitive."""

    id: str
    owner_id: int | None = None
    name: str | None = None
    tags: list[str] = field(default_factory=list)
    url: str | None = None
    filename: str | None = None
    content_type: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> FavoriteMeme:
        payload = _unwrap(data, "meme", "favorite_meme")
        return cls(
            id=str(payload.get("id") or payload.get("meme_id")),
            owner_id=_maybe_int(payload.get("owner_id") or payload.get("user_id")),
            name=payload.get("name"),
            tags=list(payload.get("tags") or []),
            url=payload.get("url"),
            filename=payload.get("filename"),
            content_type=payload.get("content_type"),
            raw_data=data,
        )


@dataclass(slots=True)
class SearchResult:
    """A Fluxer message search result page."""

    messages: list[Message] = field(default_factory=list)
    channels: list[Channel] = field(default_factory=list)
    total: int = 0
    hits_per_page: int = 25
    page: int = 1
    cursor: list[str] | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @property
    def next_cursor(self) -> list[str] | None:
        """Compatibility alias for :attr:`cursor`.

        Fluxer does not accept this cursor for pagination; use ``page`` instead.
        """
        return self.cursor

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> SearchResult:
        from .models import Channel, Message

        raw_messages = data.get("messages") or data.get("results") or []
        raw_channels = data.get("channels") or []
        channels = [
            Channel.from_data(item, http)
            for item in raw_channels
            if isinstance(item, dict)
        ]
        channels_by_id = {channel.id: channel for channel in channels}
        messages = [
            Message.from_data(item.get("message", item), http)
            for item in raw_messages
            if isinstance(item, dict)
        ]
        for message in messages:
            message._channel = channels_by_id.get(message.channel_id)

        total = data.get("total")
        if total is None:
            total = data.get("total_results", 0)
        cursor = data.get("cursor")
        return cls(
            messages=messages,
            channels=channels,
            total=_maybe_int(total) or 0,
            hits_per_page=_maybe_int(data.get("hits_per_page")) or 25,
            page=_maybe_int(data.get("page")) or 1,
            cursor=[str(item) for item in cursor] if isinstance(cursor, list) else None,
            raw_data=data,
        )


@dataclass(slots=True)
class SearchIndexing:
    """Indicates that Fluxer is preparing one or more message search indexes."""

    indexing: Literal[True] = True
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> SearchIndexing:
        return cls(raw_data=data)


SearchResponse: TypeAlias = SearchResult | SearchIndexing


def parse_search_response(
    data: dict[str, Any], http: Any | None = None
) -> SearchResponse:
    """Parse either successful body returned by message search."""
    if data.get("indexing") is True:
        return SearchIndexing.from_data(data)
    return SearchResult.from_data(data, http)


@dataclass(slots=True)
class ReadState:
    """A Fluxer read-state entry for a channel."""

    channel_id: int | None = None
    mention_count: int | None = None
    last_message_id: int | None = None
    last_pin_timestamp: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> ReadState:
        payload = _unwrap(data, "read_state")
        return cls(
            channel_id=_maybe_int(payload.get("id") or payload.get("channel_id")),
            mention_count=_maybe_int(payload.get("mention_count")),
            last_message_id=_maybe_int(payload.get("last_message_id")),
            last_pin_timestamp=payload.get("last_pin_timestamp"),
            raw_data=data,
        )


@dataclass(slots=True)
class FavoriteGif:
    """A resolved Fluxer favorite GIF/media proxy entry."""

    url: str | None = None
    proxy_url: str | None = None
    media: dict[str, Any] = field(default_factory=dict)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> FavoriteGif:
        payload = _unwrap(data, "gif", "entry")
        return cls(
            url=payload.get("url") or payload.get("source_url"),
            proxy_url=payload.get("proxy_url") or payload.get("media_proxy_url"),
            media=payload.get("media") or {},
            raw_data=data,
        )


@dataclass(slots=True)
class GiftCode:
    """A Fluxer premium gift code or gift-code metadata entry.

    User-token sensitive when retrieved from the current user's gift inventory.
    """

    code: str
    duration_type: str | None = None
    duration_quantity: int | None = None
    redeemed: bool | None = None
    created_at: str | None = None
    redeemed_at: str | None = None
    created_by: Any | None = None
    redeemed_by: Any | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> GiftCode:
        payload = _unwrap(data, "gift", "gift_code")
        from .models import User

        created_by_data = payload.get("created_by")
        redeemed_by_data = payload.get("redeemed_by")
        return cls(
            code=payload["code"],
            duration_type=payload.get("duration_type"),
            duration_quantity=_maybe_int(payload.get("duration_quantity")),
            redeemed=payload.get("redeemed")
            if payload.get("redeemed") is not None
            else payload.get("redeemed_at") is not None,
            created_at=payload.get("created_at"),
            redeemed_at=payload.get("redeemed_at"),
            created_by=User.from_data(created_by_data, http)
            if isinstance(created_by_data, dict)
            else None,
            redeemed_by=User.from_data(redeemed_by_data, http)
            if isinstance(redeemed_by_data, dict)
            else None,
            raw_data=data,
        )


@dataclass(slots=True)
class PackSummary:
    """A Fluxer emoji or sticker pack summary. User-token sensitive."""

    id: int
    name: str
    description: str | None = None
    type: str | None = None
    creator_id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    installed_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> PackSummary:
        payload = _unwrap(data, "pack")
        return cls(
            id=int(payload["id"]),
            name=payload.get("name", ""),
            description=payload.get("description"),
            type=payload.get("type"),
            creator_id=_maybe_int(payload.get("creator_id")),
            created_at=payload.get("created_at"),
            updated_at=payload.get("updated_at"),
            installed_at=payload.get("installed_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class PackDashboardSection:
    """One emoji or sticker section from Fluxer's pack dashboard."""

    installed_limit: int | None = None
    created_limit: int | None = None
    installed: list[PackSummary] = field(default_factory=list)
    created: list[PackSummary] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> PackDashboardSection:
        payload = _unwrap(data, "section")
        return cls(
            installed_limit=_maybe_int(payload.get("installed_limit")),
            created_limit=_maybe_int(payload.get("created_limit")),
            installed=[
                PackSummary.from_data(item, http)
                for item in payload.get("installed", [])
                if isinstance(item, dict)
            ],
            created=[
                PackSummary.from_data(item, http)
                for item in payload.get("created", [])
                if isinstance(item, dict)
            ],
            raw_data=data,
        )


@dataclass(slots=True)
class PackDashboard:
    """Fluxer's current-user emoji/sticker pack dashboard."""

    emoji: PackDashboardSection
    sticker: PackDashboardSection
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> PackDashboard:
        payload = _unwrap(data, "dashboard", "packs")
        return cls(
            emoji=PackDashboardSection.from_data(payload.get("emoji", {}), http),
            sticker=PackDashboardSection.from_data(payload.get("sticker", {}), http),
            raw_data=data,
        )


@dataclass(slots=True)
class EntranceSound:
    """A Fluxer entrance sound library entry. User-token sensitive."""

    id: int
    name: str
    hash: str | None = None
    extension: str | None = None
    content_type: str | None = None
    duration_ms: int | None = None
    size_bytes: int | None = None
    url: str | None = None
    created_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> EntranceSound:
        payload = _unwrap(data, "sound", "entrance_sound")
        return cls(
            id=int(payload["id"]),
            name=payload.get("name", ""),
            hash=payload.get("hash"),
            extension=payload.get("extension"),
            content_type=payload.get("content_type"),
            duration_ms=_maybe_int(payload.get("duration_ms")),
            size_bytes=_maybe_int(payload.get("size_bytes")),
            url=payload.get("url"),
            created_at=payload.get("created_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class EntranceSoundSelection:
    """A selected entrance sound for one Fluxer scope."""

    scope_id: str
    sound_id: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> EntranceSoundSelection:
        payload = _unwrap(data, "selection")
        return cls(
            scope_id=payload.get("scope_id", ""),
            sound_id=_maybe_int(payload.get("sound_id")),
            raw_data=data,
        )


@dataclass(slots=True)
class EntranceSoundLibrary:
    """Fluxer's current-user entrance sound library and active selections."""

    sounds: list[EntranceSound] = field(default_factory=list)
    selections: list[EntranceSoundSelection] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> EntranceSoundLibrary:
        payload = _unwrap(data, "library", "entrance_sounds")
        return cls(
            sounds=[
                EntranceSound.from_data(item, http)
                for item in payload.get("sounds", [])
                if isinstance(item, dict)
            ],
            selections=[
                EntranceSoundSelection.from_data(item, http)
                for item in payload.get("selections", [])
                if isinstance(item, dict)
            ],
            raw_data=data,
        )


@dataclass(slots=True)
class Theme:
    """A Fluxer custom theme create response. User-token sensitive."""

    id: str
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> Theme:
        payload = _unwrap(data, "theme")
        return cls(id=str(payload["id"]), raw_data=data)


@dataclass(slots=True)
class BanEntry:
    """A Fluxer guild ban entry."""

    user: Any
    reason: str | None = None
    moderator_id: int | None = None
    banned_at: str | None = None
    expires_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> BanEntry:
        payload = _unwrap(data, "ban")
        from .models import User

        user_data = payload.get("user")
        return cls(
            user=User.from_data(user_data, http)
            if isinstance(user_data, dict)
            else user_data,
            reason=payload.get("reason"),
            moderator_id=_maybe_int(payload.get("moderator_id")),
            banned_at=payload.get("banned_at"),
            expires_at=payload.get("expires_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class DiscoveryGuild:
    """A guild entry returned by Fluxer's discovery directory."""

    id: int | None = None
    name: str | None = None
    description: str | None = None
    member_count: int | None = None
    online_count: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> DiscoveryGuild:
        payload = _unwrap(data, "guild", "discovery_guild")
        return cls(
            id=_maybe_int(payload.get("id") or payload.get("guild_id")),
            name=payload.get("name"),
            description=payload.get("description"),
            member_count=_maybe_int(payload.get("member_count")),
            online_count=_maybe_int(payload.get("online_count")),
            raw_data=data,
        )


@dataclass(slots=True)
class VanityUrl:
    """A Fluxer guild vanity URL response."""

    code: str | None = None
    uses: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> VanityUrl:
        payload = _unwrap(data, "vanity_url", "vanity")
        return cls(
            code=payload.get("code") or payload.get("vanity_url_code"),
            uses=_maybe_int(payload.get("uses")),
            raw_data=data,
        )


@dataclass(slots=True)
class BulkOperationResult:
    """A generic Fluxer bulk-operation response wrapper."""

    items: list[Any] = field(default_factory=list)
    failed: list[Any] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> BulkOperationResult:
        return cls(
            items=list(
                data.get("items") or data.get("created") or data.get("results") or []
            ),
            failed=list(data.get("failed") or data.get("errors") or []),
            raw_data=data,
        )


@dataclass(slots=True)
class DiscoveryApplication:
    """A Fluxer guild discovery application response."""

    guild_id: int | None = None
    status: str | None = None
    category_id: str | int | None = None
    description: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> DiscoveryApplication:
        payload = _unwrap(data, "application", "discovery_application")
        return cls(
            guild_id=_maybe_int(payload.get("guild_id")),
            status=payload.get("status"),
            category_id=payload.get("category_id") or payload.get("category"),
            description=payload.get("description"),
            raw_data=data,
        )


@dataclass(slots=True)
class DiscoveryStatus:
    """A Fluxer guild discovery status response."""

    guild_id: int | None = None
    status: str | None = None
    eligible: bool | None = None
    application: DiscoveryApplication | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> DiscoveryStatus:
        payload = _unwrap(data, "discovery", "status")
        app_data = payload.get("application") or payload.get("discovery_application")
        return cls(
            guild_id=_maybe_int(payload.get("guild_id")),
            status=payload.get("status"),
            eligible=payload.get("eligible"),
            application=DiscoveryApplication.from_data(app_data, http)
            if isinstance(app_data, dict)
            else None,
            raw_data=data,
        )


@dataclass(slots=True)
class GuildTransferResult:
    """A Fluxer guild ownership transfer response."""

    guild_id: int | None = None
    owner_id: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> GuildTransferResult:
        payload = _unwrap(data, "guild")
        return cls(
            guild_id=_maybe_int(payload.get("id") or payload.get("guild_id")),
            owner_id=_maybe_int(payload.get("owner_id")),
            raw_data=data,
        )


class BulkEmojiResult(BulkOperationResult):
    """A bulk guild emoji creation response."""


class BulkStickerResult(BulkOperationResult):
    """A bulk guild sticker creation response."""


@dataclass(slots=True)
class Team:
    """A Fluxer application team-like payload, when present.

    Fluxer's current OpenAPI application schema does not define detailed
    team payloads, so this preserves any team object returned by future-
    compatible responses without inventing unsupported wire behavior.
    """

    id: int | None = None
    name: str | None = None
    owner_user_id: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> Team:
        return cls(
            id=_maybe_int(data.get("id")),
            name=data.get("name"),
            owner_user_id=_maybe_int(data.get("owner_user_id") or data.get("owner_id")),
            raw_data=data,
        )


@dataclass(slots=True)
class AppInfo:
    """A Fluxer OAuth2 application.

    This represents Fluxer application metadata where the
    OpenAPI exposes matching OAuth application fields.
    """

    id: int
    name: str
    redirect_uris: list[str] = field(default_factory=list)
    bot_public: bool = False
    bot_require_code_grant: bool = False
    client_secret: str | None = None
    bot: Any | None = None
    team: Team | None = None
    description: str | None = None
    icon: str | None = None
    verify_key: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> AppInfo:
        payload = _unwrap(data, "application", "app")
        bot_data = payload.get("bot")
        bot = None
        if isinstance(bot_data, dict):
            from .models import User

            bot = User.from_data(bot_data, http)
        team_data = payload.get("team")
        return cls(
            id=int(payload["id"]),
            name=payload.get("name", ""),
            redirect_uris=list(payload.get("redirect_uris") or []),
            bot_public=bool(payload.get("bot_public", False)),
            bot_require_code_grant=bool(payload.get("bot_require_code_grant", False)),
            client_secret=payload.get("client_secret"),
            bot=bot,
            team=Team.from_data(team_data, http)
            if isinstance(team_data, dict)
            else None,
            description=payload.get("description"),
            icon=payload.get("icon"),
            verify_key=payload.get("verify_key"),
            raw_data=data,
        )

    @property
    def owner(self) -> Any | None:
        """Return the associated bot user when Fluxer does not provide an owner."""
        return self.bot


@dataclass(slots=True)
class AuthSession:
    """A Fluxer auth session. User-token sensitive."""

    id: str | None = None
    current: bool | None = None
    ip: str | None = None
    user_agent: str | None = None
    created_at: str | None = None
    last_used_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> AuthSession:
        payload = _unwrap(data, "session", "auth_session")
        return cls(
            id=str(
                payload.get("id")
                or payload.get("session_id")
                or payload.get("auth_session_id_hash")
                or ""
            ),
            current=payload.get("current"),
            ip=payload.get("ip") or payload.get("ip_address"),
            user_agent=payload.get("user_agent"),
            created_at=payload.get("created_at"),
            last_used_at=payload.get("last_used_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class MFAState:
    """A compact Fluxer MFA state summary. User-token sensitive."""

    totp: bool | None = None
    webauthn: bool | None = None
    has_mfa: bool | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> MFAState:
        payload = _unwrap(data, "mfa", "state")
        return cls(
            totp=payload.get("totp"),
            webauthn=payload.get("webauthn"),
            has_mfa=payload.get("has_mfa"),
            raw_data=data,
        )


@dataclass(slots=True)
class WebAuthnCredential:
    """A Fluxer WebAuthn credential summary. User-token sensitive."""

    id: str | None = None
    name: str | None = None
    created_at: str | None = None
    last_used_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> WebAuthnCredential:
        payload = _unwrap(data, "credential", "webauthn_credential")
        return cls(
            id=str(payload.get("id") or payload.get("credential_id") or ""),
            name=payload.get("name"),
            created_at=payload.get("created_at"),
            last_used_at=payload.get("last_used_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class UserSettings:
    """Fluxer current-user settings. User-token sensitive."""

    theme: str | None = None
    status: str | None = None
    locale: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> UserSettings:
        payload = _unwrap(data, "settings", "user_settings")
        return cls(
            theme=payload.get("theme"),
            status=payload.get("status"),
            locale=payload.get("locale"),
            raw_data=data,
        )


@dataclass(slots=True)
class UserConnection:
    """A Fluxer linked user connection. User-token sensitive."""

    id: str | None = None
    type: str | None = None
    name: str | None = None
    verified: bool | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> UserConnection:
        payload = _unwrap(data, "connection")
        return cls(
            id=str(payload.get("id") or ""),
            type=payload.get("type"),
            name=payload.get("name"),
            verified=payload.get("verified"),
            raw_data=data,
        )


@dataclass(slots=True)
class AuthorizedIP:
    """A Fluxer authorized IP entry. User-token sensitive."""

    ip: str | None = None
    location: str | None = None
    authorized_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> AuthorizedIP:
        payload = _unwrap(data, "ip", "authorized_ip")
        return cls(
            ip=payload.get("ip") or payload.get("ip_address"),
            location=payload.get("location"),
            authorized_at=payload.get("authorized_at") or payload.get("created_at"),
            raw_data=data,
        )


@dataclass(slots=True)
class DataHarvest:
    """A Fluxer data export/harvest response. User-token sensitive."""

    id: str | None = None
    status: str | None = None
    download_url: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> DataHarvest:
        payload = _unwrap(data, "harvest", "data_harvest", "export")
        return cls(
            id=str(payload.get("id") or payload.get("request_id") or ""),
            status=payload.get("status"),
            download_url=payload.get("download_url") or payload.get("url"),
            raw_data=data,
        )


@dataclass(slots=True)
class CallEligibility:
    """A Fluxer call eligibility response."""

    channel_id: int | None = None
    can_call: bool | None = None
    ringable: bool | None = None
    active: bool | None = None
    silent: bool | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> CallEligibility:
        payload = _unwrap(data, "eligibility", "call")
        return cls(
            channel_id=_maybe_int(payload.get("channel_id")),
            can_call=payload.get("can_call")
            if "can_call" in payload
            else payload.get("eligible"),
            ringable=payload.get("ringable"),
            active=payload.get("active") or payload.get("has_active_call"),
            silent=payload.get("silent") or payload.get("silent_mode"),
            raw_data=data,
        )


@dataclass(slots=True)
class RTCRegion:
    """A Fluxer RTC region entry."""

    id: str | None = None
    name: str | None = None
    optimal: bool | None = None
    deprecated: bool | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> RTCRegion:
        payload = _unwrap(data, "region", "rtc_region")
        return cls(
            id=payload.get("id"),
            name=payload.get("name"),
            optimal=payload.get("optimal"),
            deprecated=payload.get("deprecated"),
            raw_data=data,
        )


@dataclass(slots=True)
class CallState:
    """A compact Fluxer call operation response."""

    channel_id: int | None = None
    region: str | None = None
    active: bool | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> CallState:
        payload = _unwrap(data, "call", "state")
        return cls(
            channel_id=_maybe_int(payload.get("channel_id")),
            region=payload.get("region") or payload.get("rtc_region"),
            active=payload.get("active"),
            raw_data=data,
        )


@dataclass(slots=True)
class VoiceDebugSession:
    """A Fluxer voice debug logging session/status response."""

    channel_id: int | None = None
    enabled: bool | None = None
    session_id: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(
        cls, data: dict[str, Any], http: Any | None = None
    ) -> VoiceDebugSession:
        payload = _unwrap(data, "session", "voice_debug_session")
        return cls(
            channel_id=_maybe_int(payload.get("channel_id")),
            enabled=payload.get("enabled")
            if "enabled" in payload
            else payload.get("active"),
            session_id=payload.get("session_id") or payload.get("id"),
            raw_data=data,
        )


@dataclass(slots=True)
class SlowmodeState:
    """A Fluxer channel slowmode state response."""

    channel_id: int | None = None
    interval: int | None = None
    next_allowed_at: str | None = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict[str, Any], http: Any | None = None) -> SlowmodeState:
        payload = _unwrap(data, "slowmode", "state")
        return cls(
            channel_id=_maybe_int(payload.get("channel_id")),
            interval=_maybe_int(
                payload.get("interval") or payload.get("rate_limit_per_user")
            ),
            next_allowed_at=payload.get("next_allowed_at"),
            raw_data=data,
        )
