# Changelog

## [0.5.0-alpha.1] (development) - 2026-03-30

### Added

- New API usage: message search

## [0.5.0] (development) - 2026-03-30

### Added, Changed & Fixed

Initial rework
- Added, changed and fixed a LOT of things.

## [0.4.2] - 2026-03-30

### Added
- Added `escape_mentions` utility method (#73)
- Added subclassed bot example (#72)
- Added basic bot example (#71)
- Added setup hook (#67)
- Added `from_dict` method to Embeds class (#65)
- Added issue templates (#63)
- Added `Client.get_guild` method (#59)
- Added contribution guidelines (#58, #61)
- Added `ban_duration_seconds` parameter to `Guild.ban` (#60)
- Added support for triggering typing indicator in channels (#54)
- Added support for custom prefix handlers for Bot (#53)
- Added message pin and unpin functionality (#46)
- Added support for referenced messages in Message Model (#42)

### Changed
- Standardized embed handling for `message.send` and `channel.send` (#64)
- Improved guild caching and added reply caching (#47)

### Fixed
- Fixed closed sockets being silently dropped (#68)
- Fixed name representation for unicode emojis (#57)
- Fixed commands sorting for cogs (#57)
- Applied checks to development branch
- Fixed ruff format issues

### Documentation
- Improvements for repository contributions (#61)
- Added contributing guidelines (#51, #58)