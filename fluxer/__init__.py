__version__ = "0.4.2"
__title__ = "fluxer.py"
__author__ = "Emil"
__license__ = "MIT"

# Core classes
from .client import Bot, Client, when_mentioned_or, when_mentioned
from .cog import Cog
from .enums import ChannelType, GatewayCloseCode, GatewayOpcode, Intents, Permissions
from .file import File
from .fluxer_models import (
    SearchAuthorType,
    SearchContentType,
    SearchEmbedType,
    SearchIndexing,
    SearchResponse,
    SearchResult,
    SearchScope,
    SearchSortBy,
    SearchSortOrder,
)
from .http import HTTPClient

# Checks
from .activity import Activity, BaseActivity, CustomActivity, Game, Spotify, Streaming
from .checks import has_role, has_permission
from .colour import Color, Colour
from .mentions import AllowedMentions
from .object import Object

# Errors
from .errors import (
    BadRequest,
    FluxerException,
    Forbidden,
    GatewayException,
    GatewayNotConnected,
    HTTPException,
    LoginFailure,
    NotFound,
    RateLimited,
    Unauthorized,
)

# Models
from .models import (
    Channel,
    Embed,
    Emoji,
    Guild,
    GuildMember,
    Message,
    MessageReference,
    DeletedReferencedMessage,
    PartialMessage,
    Reaction,
    Role,
    User,
    UserProfile,
    VoiceState,
    Webhook,
    WebhookMessage,
)

# Voice support is optional so only import if available
try:
    from .voice import FFmpegPCMAudio, VoiceClient
except ImportError:
    pass

# Utilities
from .utils import datetime_to_snowflake, snowflake_to_datetime

__all__ = [
    # Checks
    "Activity",
    "BaseActivity",
    "CustomActivity",
    "Game",
    "Spotify",
    "Streaming",
    "has_role",
    "has_permission",
    "AllowedMentions",
    "Color",
    "Colour",
    "Object",
    # Client
    "Bot",
    "Client",
    "when_mentioned",
    "when_mentioned_or",
    "Cog",
    "File",
    "HTTPClient",
    # Enums
    "ChannelType",
    "GatewayCloseCode",
    "GatewayOpcode",
    "Intents",
    "Permissions",
    "SearchAuthorType",
    "SearchContentType",
    "SearchEmbedType",
    "SearchScope",
    "SearchSortBy",
    "SearchSortOrder",
    # Errors
    "BadRequest",
    "FluxerException",
    "Forbidden",
    "GatewayException",
    "GatewayNotConnected",
    "HTTPException",
    "LoginFailure",
    "NotFound",
    "RateLimited",
    "Unauthorized",
    # Models
    "Channel",
    "Embed",
    "Emoji",
    "Guild",
    "GuildMember",
    "Message",
    "MessageReference",
    "DeletedReferencedMessage",
    "PartialMessage",
    "Reaction",
    "Role",
    "SearchIndexing",
    "SearchResponse",
    "SearchResult",
    "User",
    "UserProfile",
    "VoiceState",
    "Webhook",
    "WebhookMessage",
    # Utils
    "datetime_to_snowflake",
    "snowflake_to_datetime",
    # Voice (present only when the 'voice' extra is installed)
    "FFmpegPCMAudio",
    "VoiceClient",
]
