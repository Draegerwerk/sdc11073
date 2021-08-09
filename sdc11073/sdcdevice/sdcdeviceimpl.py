import copy

from .. import pmtypes


class SdcDevice:
    defaultInstanceIdentifiers = (pmtypes.InstanceIdentifier(root='rootWithNoMeaning', extension_string='System'),)

    def __init__(self, ws_discovery, my_uuid, model, device, device_mdib_container, validate=True, roleProvider=None,
                 ssl_context=None,
                 max_subscription_duration=7200, log_prefix='', specific_components=None,
                 chunked_messages=False):  # pylint:disable=too-many-arguments
        # ssl protocol handling itself is delegated to a handler.
        # Specific protocol versions or behaviours are implemented there.
        self._components = copy.deepcopy(device_mdib_container.sdc_definitions.DefaultSdcDeviceComponents)
        if specific_components is not None:
            # merge specific stuff into _components
            for key, value in specific_components.items():
                self._components[key] = value
        handler_cls = self._components['SdcDeviceHandlerClass']
        self._handler = handler_cls(my_uuid, ws_discovery, model, device, device_mdib_container, validate,
                                    roleProvider, ssl_context, max_subscription_duration,
                                    self._components,
                                    log_prefix=log_prefix, chunked_messages=chunked_messages)
        self._wsdiscovery = ws_discovery
        self._logger = self._handler._logger
        self._mdib = device_mdib_container
        self._location = None

    def set_location(self, location, validators=defaultInstanceIdentifiers, publish_now=True):
        '''
        @param location: a pysdc.location.SdcLocation instance
        @param validators: a list of pmtypes.InstanceIdentifier objects or None; in that case the defaultInstanceIdentifiers member is used
        @param publish_now: if True, the device is published via its wsdiscovery reference.
        '''
        if location == self._location:
            return

        if self._location is not None:
            self._wsdiscovery.clear_service(self.epr)

        self._location = location

        if location is None:
            return

        self._mdib.set_location(location, validators)
        if publish_now:
            self.publish()

    def publish(self):
        """
        publish device on the network (sends HELLO message)
        :return:
        """
        scopes = self._handler.mk_scopes()
        x_addrs = self.get_xaddrs()
        self._wsdiscovery.publish_service(self.epr, self._mdib.sdc_definitions.MedicalDeviceTypesFilter, scopes, x_addrs)

    @property
    def shall_validate(self):
        return self._handler._validate

    @property
    def mdib(self):
        return self._mdib

    @property
    def subscriptions_manager(self):
        return self._handler._subscriptions_manager

    @property
    def sco_operations_registry(self):
        return self._handler._sco_operations_registry

    @property
    def epr(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._handler._my_uuid.urn

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._handler.path_prefix

    def register_operation(self, operation):
        return self._handler.register_operation(operation)

    def unregister_operation_by_handle(self, operation_handle):
        return self._handler.unregister_operation_by_handle(operation_handle)

    def get_operation_by_handle(self, operation_handle):
        return self._handler.get_operation_by_handle(operation_handle)

    def enqueue_operation(self, operation, request, argument):
        return self._handler.enqueue_operation(operation, request, argument)

    def dispatch_get_request(self, parse_result, headers):
        ''' device itself can also handle GET requests. This is the handler'''
        return self._handler.dispatch_get_request(parse_result, headers)

    def start_all(self, start_rtsample_loop=True, periodic_reports_interval=None, shared_http_server=None):
        """

        :param start_rtsample_loop: flag
        :param periodic_reports_interval: if provided, a value in seconds
        :param shared_http_server: id provided, use this http server. Otherwise device creates its own.
        :return:
        """
        return self._handler.start_all(start_rtsample_loop, periodic_reports_interval, shared_http_server)

    def stop_all(self, close_all_connections=True, send_subscription_end=True):
        return self._handler.stop_all(close_all_connections, send_subscription_end)

    def stop_realtime_sample_loop(self):
        return self._handler.stop_realtime_sample_loop()

    def get_xaddrs(self):
        return self._handler.get_xaddrs()

    def send_metric_state_updates(self, mdib_version, states):
        return self._handler.send_metric_state_updates(mdib_version, states)

    def send_alert_state_updates(self, mdib_version, states):
        return self._handler.send_alert_state_updates(mdib_version, states)

    def send_component_state_updates(self, mdib_version, states):
        return self._handler.send_component_state_updates(mdib_version, states)

    def send_context_state_updates(self, mdib_version, states):
        return self._handler.send_context_state_updates(mdib_version, states)

    def send_operational_state_updates(self, mdib_version, states):
        return self._handler.send_operational_state_updates(mdib_version, states)

    def send_realtime_samples_state_updates(self, mdib_version, states):
        return self._handler.send_realtime_samples_state_updates(mdib_version, states)

    def send_descriptor_updates(self, mdib_version, updated, created, deleted, states):
        return self._handler.send_descriptor_updates(mdib_version, updated, created, deleted, states)

    def set_used_compression(self, *compression_methods):
        return self._handler.set_used_compression(*compression_methods)

    @property
    def product_roles(self):
        return self._handler.product_roles

    @product_roles.setter
    def product_roles(self, product_roles):
        self._handler.product_roles = product_roles
