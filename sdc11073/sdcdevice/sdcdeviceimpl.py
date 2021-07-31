import copy
from .. import pmtypes


class SdcDevice:
    defaultInstanceIdentifiers = (pmtypes.InstanceIdentifier(root='rootWithNoMeaning', extension_string='System'),)

    def __init__(self, ws_discovery, my_uuid, model, device, deviceMdibContainer, validate=True, roleProvider=None, sslContext=None,
                 logLevel=None, max_subscription_duration=7200, log_prefix='', specific_components=None,
                 chunked_messages=False): #pylint:disable=too-many-arguments
        # ssl protocol handling itself is delegated to a handler.
        # Specific protocol versions or behaviours are implemented there.
        self._components = copy.deepcopy(deviceMdibContainer.sdc_definitions.DefaultSdcDeviceComponents)
        if specific_components is not None:
            # merge specific stuff into _components
            for key, value in  specific_components.items():
                self._components[key] = value
        handler_cls = self._components['SdcDeviceHandlerClass']
        self._handler = handler_cls(my_uuid, ws_discovery, model, device, deviceMdibContainer, validate,
                                    roleProvider, sslContext, logLevel, max_subscription_duration,
                                    self._components,
                                    log_prefix=log_prefix, chunked_messages=chunked_messages)
        self._wsdiscovery = ws_discovery
        self._logger = self._handler._logger
        self._mdib = deviceMdibContainer
        self._location = None

    def setLocation(self, location, validators=defaultInstanceIdentifiers, publishNow=True):
        '''
        @param location: a pysdc.location.SdcLocation instance
        @param validators: a list of pmtypes.InstanceIdentifier objects or None; in that case the defaultInstanceIdentifiers member is used
        @param publishNow: if True, the device is published via its wsdiscovery reference.
        '''
        if location == self._location:
            return

        if self._location is not None:
            self._wsdiscovery.clear_service(self.epr)

        self._location = location

        if location is None:
            return

        self._mdib.setLocation(location, validators)
        if publishNow:
            self.publish()

    def publish(self):
        """
        publish device on the network (sends HELLO message)
        :return:
        """
        scopes = self._handler.mkScopes()
        xAddrs = self.getXAddrs()
        self._wsdiscovery.publish_service(self.epr, self._mdib.sdc_definitions.MedicalDeviceTypesFilter, scopes, xAddrs)


    @property
    def shallValidate(self):
        return self._handler._validate

    @property
    def mdib(self):
        return self._mdib

    @property
    def subscriptionsManager(self):
        return self._handler._subscriptionsManager

    @property
    def scoOperationsRegistry(self):
        return self._handler._scoOperationsRegistry

    @property
    def epr(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._handler._my_uuid.urn

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._handler.path_prefix

    def registerOperation(self, operation):
        return self._handler.registerOperation(operation)

    def unRegisterOperationByHandle(self, operationHandle):
        return self._handler.unRegisterOperationByHandle(operationHandle)

    def getOperationByHandle(self, operationHandle):
        return self._handler.getOperationByHandle(operationHandle)

    def enqueueOperation(self, operation, request, argument):
        return self._handler.enqueueOperation(operation, request, argument)

    def dispatchGetRequest(self, parseResult, headers):
        ''' device itself can also handle GET requests. This is the handler'''
        return self._handler.dispatchGetRequest(parseResult, headers)

    def startAll(self, startRealtimeSampleLoop=True, periodic_reports_interval=None, shared_http_server = None):
        """

        :param startRealtimeSampleLoop: flag
        :param periodic_reports_interval: if provided, a value in seconds
        :param shared_http_server: id provided, use this http server. Otherwise device creates its own.
        :return:
        """
        return self._handler.startAll(startRealtimeSampleLoop, periodic_reports_interval, shared_http_server)

    def stopAll(self, closeAllConnections=True, sendSubscriptionEnd=True):
        return self._handler.stopAll(closeAllConnections, sendSubscriptionEnd)

    def stop_realtime_sample_loop(self):
        return self._handler.stop_realtime_sample_loop()

    def getXAddrs(self):
        return self._handler.getXAddrs()


    def sendMetricStateUpdates(self, mdib_version, stateUpdates):
        return self._handler.sendMetricStateUpdates(mdib_version, stateUpdates)

    def sendAlertStateUpdates(self, mdib_version, stateUpdates):
        return self._handler.sendAlertStateUpdates(mdib_version, stateUpdates)

    def sendComponentStateUpdates(self, mdib_version, stateUpdates):
        return self._handler.sendComponentStateUpdates(mdib_version, stateUpdates)

    def sendContextStateUpdates(self, mdib_version, stateUpdates):
        return self._handler.sendContextStateUpdates(mdib_version, stateUpdates)

    def sendOperationalStateUpdates(self, mdib_version, stateUpdates):
        return self._handler.sendOperationalStateUpdates(mdib_version, stateUpdates)

    def sendRealtimeSamplesStateUpdates (self, mdib_version, stateUpdates):
        return self._handler.sendRealtimeSamplesStateUpdates(mdib_version, stateUpdates)

    def sendDescriptorUpdates(self, mdib_version, updated, created, deleted, updated_states):
        return self._handler.sendDescriptorUpdates(mdib_version, updated, created, deleted, updated_states)

    def sendWaveformUpdates(self, changedSamples):
        return self._handler.sendWaveformUpdates(changedSamples)

    def setUsedCompression(self, *compression_methods):
        return self._handler.setUsedCompression(*compression_methods)

    @property
    def product_roles(self):
        return self._handler.product_roles

    @product_roles.setter
    def product_roles(self, product_roles):
        self._handler.product_roles = product_roles
