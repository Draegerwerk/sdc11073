# Changelog
All notable changes to the sdc11073 module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.26] - 2023-06-08

### Added

- Option to configure the time to wait until the http server is started [#142](https://github.com/Draegerwerk/sdc11073/issues/142)
- Enum for ChargeStatus of battery state [#165](https://github.com/Draegerwerk/sdc11073/issues/165).

## [1.1.25] - 2023-04-21

### Added

- Added previously removed `nodeName` index

## [1.1.24] - 2023-04-16

### Added
- `T_CalibrationResult`
- `T_CalibrationDocumentation`
- `T_CalibrationState`
- `T_CalibrationType`
- `CalibrationInfo`
- Codecov config

## [1.1.23] - 2023-04-12
### Fixed
- error in constructor of WsDiscoverySingleAdapter
- fixed error in alarmprovider.py, handling of fallback signals
- added enum AlertSignalPrimaryLocation

## [1.1.22] - 2023-03-23
### Changed
- replaces netifaces with ifaddr
- removed option to run single threaded http server

### Fixed
- make weak ref to mdib a normal ref in sdc client
- update observables in mkStateContainersforAllDescriptors
- make copies of reportNode before processing in clientmdib

## [1.1.21] - 2023-02-24
### Fixed
- SourceMds being interpreted as a state with an DescriptorHandle attribute

## [1.1.20] - 2022-12-12
### Fixed
- AllowedValue.Value is not optional
- AlertSystemState LastSelfCheck and SelfCheckCount are only updated in self check cycle, 
  not when part of transaction due to an AlertConditionState change

## [1.1.19] - 2022-12-08
### Fixed
- iteration over DPWSHosted.types

## [1.1.18] - 2022-11-29
### Added
- InstanceId is handled in mdib


## [1.1.17] - 2022-11-15
### Fixed
- update *ByHandle observables depending on the type of the updated state
- in MdibBase.contextByHandle dictionary use Handle as key instead of DescriptorHandle
- in SSLSocket wrap_socket calls set do_handshake_on_connect=False to avoid possible blocking
### Added
- method to represent LocalizedText objects as string 
- alert delecation acc. to BICEPS chapter 6.2


## [1.1.16] - 2022-09-30
### Fixed
- fixed getMetricDescriptorByCode method (translations were not handled)
### Added
- method have_matching_codes to compare COdedValue instances
- extend some constructors to add elements to the Extension element of a descriptor

## [1.1.15] - 2022-09-20
### Fixed
- fixed missing updates in alertsByHandle, metricsByHandle.... after DescriptionsModificationReports.

## [1.1.14] - 2022-09-12
### Changed
- refactor wsdiscovery, moved a lot of functions to a new class MessageCreator
- make udp multicast port an optional argument to constructor. This allows (especially in tests) to use a different port.

## [1.1.13] - 2022-08-09
### Fixed
- fixed bug introduced in version 1.1.12, in OperationGroup.fromNode

## [1.1.12] - 2022-08-05
### Changed
- keep xml representation of a property when updating from xml data. Only when application sets a new value, original xml representation becomes invalid.

### Fixed
- fixed wrong data type in updateDescrFromNode
- too many warnings logged in determinationtime checking

## [1.1.11] - 2022-06-17
### Changed
- log an error when an unexpected mdib version was received, keep Notifications with same/older mdib versions with warning

## [1.1.10] - 2022-03-17
### Added
- add html unit test runner 

### Fixed
- fixed possible bug caused by implicit reopen of http connections in soap client.

## [1.1.9] - 2022-02-09
### Fixed
- fixed broken discovery on posix systems (bug since version 1.1.6)

## [1.1.8] - 2022-02-04
### Added
- add callback to observe WS-Discovery ProbeMatch messages

## [1.1.7] - 2022-02-02
### Added
- support of Retrievability Extension

- possible invalid file name in communication log.
- dispatching SOAP notifications by ReferenceParameters using mandatory attribute IsReferenceParameter

### Changed
- xml schema validation validates complete soap envelopes, not only the body (as before).
- allow changing of pmtypes.DefaultCodingSystem at runtime

## [1.1.6] - 2022-01-27
### Fixed
  - wsdiscovery no longer listens on all adapters on multicast address
  - in wsdiscovery: stopping the sending thread not until its queue data is processed.  
    This prevents that bye messages are not send.

## [1.1.5] - 2021-11-26
### Added
- observable property for SubscriptionEnd Messages
### Fixed
- use MetadataVersion in WsDiscovery
- fix setLocation for SdcDevice implementation - no Bye-message on location change
- fixed comparison regarding float precision in NumericMetricValue.__eq__ and SampleArrayValue.__eq__
- fixed formatting error in isoduration.date_time_string when datetime object has microseconds

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
- fixed wsdiscovery problems in high load scenarios (possible deadlocks, performance degradation)
- fixed broken enabling of communication logger
 
### Added
- support for discovery proxy over http(s)
- periodic reports implemented

### Changed
- application can provide own waveform generation mechanics
