# Changelog
All notable changes to the sdc11073 module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2021-04-22
### Fixed
- fixed bug if context descriptor is updated, obsolete context states were not correctly identified and deleted.
- some updates on reference test code

## [1.1.0] - 2021-03-12
### Fixed
- fixed wsdiscovery problems in high load scenarios (possible deadlocks, performance degrations)
- fixed broken enabling of communication logger
 
### Added
- support for discovery proxy over http(s)
- periodic reports implemented

### Changed
- application can provide own waveform generation mechanics
