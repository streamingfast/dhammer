# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added option `SetInChanSize` to `NewHammer` which can be used to control the maximum size of `In` channel.
- Added debug log statements in library for easier troubleshooting. Chatty debug log are printed only when environment variable `TRACE=true` is set.