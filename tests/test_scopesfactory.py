"""Unittest for scopes factory module."""

import unittest
import uuid
from unittest import mock

from sdc11073.location import SdcLocation
from sdc11073.mdib import statecontainers
from sdc11073.provider.scopesfactory import KEY_PURPOSE_SERVICE_PROVIDER, mk_scopes
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.wsd_types import ScopesType


class TestMkScopes(unittest.TestCase):
    def setUp(self):
        # Mock the ProviderMdibProtocol and its attributes
        self.mdib = mock.MagicMock()
        self.mdib.data_model.pm_types.ContextAssociation.ASSOCIATED = pm_types.ContextAssociation.ASSOCIATED
        self.mdib.data_model.pm_names.LocationContextDescriptor = 'LocationContextDescriptor'
        self.mdib.data_model.pm_names.OperatorContextDescriptor = 'OperatorContextDescriptor'
        self.mdib.data_model.pm_names.EnsembleContextDescriptor = 'EnsembleContextDescriptor'
        self.mdib.data_model.pm_names.WorkflowContextDescriptor = 'WorkflowContextDescriptor'
        self.mdib.data_model.pm_names.MeansContextDescriptor = 'MeansContextDescriptor'
        self.mdib.data_model.pm_names.MdsDescriptor = 'MdsDescriptor'

    def test_no_associated_locations(self):
        """Test when there are no associated locations."""
        result = mk_scopes(self.mdib)
        self.assertIsInstance(result, ScopesType)
        self.assertEqual(len(result.text), 1)  # Only the default key purpose scope
        self.assertEqual(result.text[0], KEY_PURPOSE_SERVICE_PROVIDER)

    def test_single_associated_location(self):
        """Test with a single associated location."""
        root = uuid.uuid4().hex
        sdc_location = SdcLocation(
            root=root,
            fac=uuid.uuid4().hex,
            poc=uuid.uuid4().hex,
            bed=uuid.uuid4().hex,
            bldng=uuid.uuid4().hex,
            flr=uuid.uuid4().hex,
            rm=uuid.uuid4().hex,
        )
        loc_state = statecontainers.LocationContextStateContainer(
            mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
            uuid.uuid4().hex,
        )
        loc_state.update_from_sdc_location(sdc_location)
        self.mdib.entities.by_node_type.return_value = [mock.MagicMock(states={uuid.uuid4().hex: loc_state})]
        result = mk_scopes(self.mdib)
        self.assertEqual(
            sdc_location.scope_string,
            result.text[0],
        )

    def test_context_associations(self):
        root = uuid.uuid4().hex
        extension = uuid.uuid4().hex
        # Test with multiple context associations
        mock_state = mock.MagicMock()
        mock_state.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        mock_state.Identification = [mock.MagicMock(Root=root, Extension=extension)]
        self.mdib.entities.by_node_type.side_effect = (
            lambda nodetype: [
                mock.MagicMock(states={'state1': mock_state}),
            ]
            if nodetype == 'OperatorContextDescriptor'
            else []
        )

        result = mk_scopes(self.mdib)
        self.assertIn(f'sdc.ctxt.opr:/{root}/{extension}', result.text)

    def test_device_component_based_scopes(self):
        """Test device component-based scopes."""
        mock_entity = mock.MagicMock()
        mock_entity.descriptor.Type.CodingSystem = uuid.uuid4().hex
        mock_entity.descriptor.Type.CodingSystemVersion = uuid.uuid4().hex
        mock_entity.descriptor.Type.Code = uuid.uuid4().hex
        self.mdib.entities.by_node_type.side_effect = (
            lambda nodetype: [
                mock_entity,
            ]
            if nodetype == 'MdsDescriptor'
            else []
        )

        result = mk_scopes(self.mdib)
        self.assertIn(
            f'sdc.cdc.type:/'
            f'{mock_entity.descriptor.Type.CodingSystem}/'
            f'{mock_entity.descriptor.Type.CodingSystemVersion}/'
            f'{mock_entity.descriptor.Type.Code}',
            result.text,
        )

    def test_raise_error_if_no_identification_element(self):
        """Test that an error is raised if a location state has no Identification."""
        loc_state = statecontainers.LocationContextStateContainer(
            mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
            uuid.uuid4().hex,
        )
        loc_state.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        self.mdib.entities.by_node_type.return_value = [mock.MagicMock(states={uuid.uuid4().hex: loc_state})]

        with self.assertRaises(ValueError) as context:
            mk_scopes(self.mdib)

        self.assertEqual(
            f'State {loc_state.Handle} of type {self.mdib.data_model.pm_names.LocationContextDescriptor} has no '
            f'Identification element',
            str(context.exception),
        )

    def test_raise_error_if_no_location_detail_element(self):
        """Test that an error is raised if a location state has no LocationDetail."""
        loc_state = statecontainers.LocationContextStateContainer(
            mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
            uuid.uuid4().hex,
        )
        loc_state.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        loc_state.Identification = [
            pm_types.InstanceIdentifier(root=uuid.uuid4().hex, extension_string=uuid.uuid4().hex),
        ]
        loc_state.LocationDetail = None
        self.mdib.entities.by_node_type.return_value = [mock.MagicMock(states={uuid.uuid4().hex: loc_state})]

        with self.assertRaises(ValueError) as context:
            mk_scopes(self.mdib)

        self.assertEqual(
            f'State {loc_state.Handle} of type {self.mdib.data_model.pm_names.LocationContextDescriptor} has no '
            f'LocationDetail element',
            str(context.exception),
        )
