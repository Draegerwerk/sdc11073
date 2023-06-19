# Changelog

All notable changes to the sdc11073 module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [master]

## [2.0.0a4] - 2023-06-16

### Changed
- reworked wsdiscovery. Wsdiscovery can now only bind to a single ip address. 
  The only available classes are "WsDiscovery" and "WsDiscoverySingleAdapter".

### Fixed
- fixed bug that roundtrip statistics raises an exception when no data is available.
- fixed possible exception that "_short_filter_names" does not exist
- fixed missing namespace for IsReferenceParameter attribute
 
## [2.0.0a3] - 2023-06-08

### Added
- SdcClient.start_all has optional parameter "check_get_service"
- 
### Changed
- moved port type name declaration from components.py to implementation classes [#176](https://github.com/Draegerwerk/sdc11073/issues/176)

### Fixed
- fixed bug in handling of soap faults (faultcodeEnum must be QNames, not strings)

## [2.0.0a2] - 2023-06-07

### Fixed
- fixed bug that mk_scopes only created a scope if a location was associated.
- fixed broken fault message generation in subscriptionsmanager_base. A previously removed method of 
namespace helper was still used. 
- fixed bug in constructor of dpws_types.ThisDeviceType. This could cause an invalid xml:lang attribute. 
- fixed bug that ContainerBase.node was not set in update_from_node

## [2.0.0a1] - 2023-06-01

### Changed
- Code follows PEP8 style (mostly)
- Using dependency injection, classes are defined in sdcdevice/components.py and sdcclients/components.py.
This allows users to replace components withs own ones and modify the behavior of client and device.
- All QNames of BICEPS participant model, message model and extension are predefined in 
pm_qnames.py, msg_qnames.py and ext_qnames.py. Users no longer need helper functions like domTag, msgTag, etc that
were needed in version 1.
- A lot of internal functionality was refactoring or rewritten.
- The API for role providers changed.
- The transactions API is simplified.
- code in repo is located under a src parent folder. 
- A lot of small changes and fixes, too many to list them all.

### Added
- Multiple SdcClient instances can use a shared HTTP Server.
- strict type checking for participant model data. When assigning a value, the data is checked and 
an exception is thrown if type is not as expected.
- Having multiple MDS in a single mdib is supported.

