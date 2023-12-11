# Changelog

All notable changes to the sdc11073 module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- added a way to process operations directly (directly send 'Fin' instead of Wait, Started,...)
- added handling of SystemErrorReports.

### Fixed
- basic_logging_setup only handles sdc logger, no more side effect due to calling logging.basicConfig. 
- fix possible invalid prefix if QName is a node text.
- fixed wrong response for SetContextState message. [#287](https://github.com/Draegerwerk/sdc11073/issues/287
- fixed connection problem when provider closes socket after first request. [#289](https://github.com/Draegerwerk/sdc11073/issues/289
- change default in ContainerBase.mk_copy to not copy node due to performance problems. [#294](https://github.com/Draegerwerk/sdc11073/issues/294
- waveform provider too slow

### Changed
- change python classes of `addressing_types.py` to match ws-addressing standard of 2006 instead of 2004 
- The final OperationInvokedReport has OperationTargetRef parameter set. 
  This required refactoring of Operations handling.
- moved waveform generator from mdib to roles.waveformprovider
- alert provider performs self check one second before SelfCheckInterval elapses

## [2.0.0a6] - 2023-09-11

### Added

- `network` module to handle network adapter stuff of the host computer
- `mypy` static code analysis

### Fixed

- possible choosing wrong ipaddress/network interface [#187](https://github.com/Draegerwerk/sdc11073/issues/187)
- added missing SerialNumber to ThisDeviceType
- no creation of operation target states, they should already exist or are not needed if multi state.
- transaction id is unique for provider instead only of sco. 
- fixed problem that on operations without handler the transaction id always was 0.
- consumer: renew could be sent after unsubscribe
- possible deadlock when stopping provider
- fixed a bug where the `SdcConsumer` failed to determine the host network adapter if the ip contained in the `device_location` is on a different subnet
- comparison of extensions would fail [#238](https://github.com/Draegerwerk/sdc11073/issues/238)
- ExtensionLocalValue.value must be a list instead of a dictionary in order to allow multiple elements with same name.
- fixed a bug where namespaces of xml are lost when coping lxml elements


### Changed

- when creating a `SdcClient` with a `device_location` or `WsDiscovery` containing an ip where no suitable host network adapter could be determined from, an `NetworkAdapterNotFoundError` is raised
- removed `netconn` module
- renamed Device with Provider in order to be more compliant with sdc11073 names: 
  sdc11073.sdcdevice.SdcDevice becomes sdc11073.provider.SdcProvider,
  sdc11073.mdib.devicemdib.DeviceMdibContainer becomes sdc11073.mdib.providermdib.ProviderMdib, etc.
- renamed Client with Consumer: sdc11073.sdcclient.SdcClient becomes sdc11073.consumer.SdcConsumer,
  sdc11073.mdib.clientmdib.ClientMdibContainer becomes sdc11073.mdib.consumermdib.ConsumerMdib, etc.
- reduced max_subscription_duration of provider to 15 seconds
- renew can be performed in fixed intervals (as before) or depending on Expires time of subscription.
- SdcLocation class reworked; use 'bldng' instead of 'bld', which better matches the standard. 
- Some classes renamed in pmtypes.py
- soap client does not try implicit reconnects
- replaced some of the ssl_context parameters with an ssl_context_container parameter, 
  that can hold two ssl context objects, to be able to get rid of 
  the deprecation warning occurring when using the same ssl context for both client and server side
- replaced AbstractDescriptorContainer.retrievability property with methods get_retrievability() and set_retrievability()


## [2.0.0a5] - 2023-06-27

### Fixed
- improved error handling when reading attribute values from xml element
- fixed possible exception in calculation of waveform age
- fixed some places where default_py_value should be implied_py_value
- fixed incomplete modeling of ClinicalInfo class

### Added
- added switch to allow disabling strict checking for AppSequence.

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

