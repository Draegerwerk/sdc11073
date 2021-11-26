# Changelog
All notable changes to the sdc11073 module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.5] - 2021-11-18
### Fixed
- use MetadataVersion in Wsdiscovery
- fix setLocation for SdcDevice implementation - no Bye-message on location change
- fixed comparison regarding float precision in NumericMetricValue.__eq__ and SampleArrayValue.__eq__

## [1.1.4] - 2021-10-15
### Fixed
- fixed possible Exception in SampleArrayValue.__eq__
- fixed issue #30: Remove or clarify sdc11073/ca
- fixed issue #29: Allow disabling plain connections entirely

## [1.1.3] - 2021-09-10
### Fixed
- removed warning if in sdc client a notification receiver thread could not be terminated 
  within a second. This can happen because closing of a socket can take some time. 
- removed dependencies lz4 and cryptography
- fixed issue #45: Update list of properties for AbstractOperationDescriptorContainer class

## [1.1.2] - 2021-08-23
### Fixed
- fixed handling of AbstractOperationDescriptorContainer.AccessLevel
- Python 3.8 support added in setup.py

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
