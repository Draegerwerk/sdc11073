"""Unittest for scopes factory module."""

import unittest
import uuid
from unittest import mock

from sdc11073.location import SdcLocation
from sdc11073.provider.scopesfactory import mk_scopes
from sdc11073.xml_types.wsd_types import ScopesType


class TestMkScopes(unittest.TestCase):
    def setUp(self):
        self.pm_types_associated = uuid.uuid4().hex
        # Mock the ProviderMdibProtocol and its attributes
        self.mdib = mock.MagicMock()
        self.mdib.data_model.pm_types.ContextAssociation.ASSOCIATED = self.pm_types_associated
        self.mdib.data_model.pm_names.LocationContextDescriptor = 'LocationContextDescriptor'
        self.mdib.data_model.pm_names.OperatorContextDescriptor = 'OperatorContextDescriptor'
        self.mdib.data_model.pm_names.EnsembleContextDescriptor = 'EnsembleContextDescriptor'
        self.mdib.data_model.pm_names.WorkflowContextDescriptor = 'WorkflowContextDescriptor'
        self.mdib.data_model.pm_names.MeansContextDescriptor = 'MeansContextDescriptor'
        self.mdib.data_model.pm_names.MdsDescriptor = 'MdsDescriptor'

    def test_no_associated_locations(self):
        # Test when there are no associated locations
        self.mdib.entities.by_node_type.return_value = []
        result = mk_scopes(self.mdib)
        self.assertIsInstance(result, ScopesType)
        self.assertEqual(len(result.text), 1)  # Only the default key purpose scope

    def test_single_associated_location(self):
        # Test with a single associated location
        mock_location = mock.MagicMock()
        mock_location.ContextAssociation = self.pm_types_associated
        mock_location.LocationDetail.Facility = uuid.uuid4().hex
        mock_location.LocationDetail.PoC = uuid.uuid4().hex
        mock_location.LocationDetail.Bed = uuid.uuid4().hex
        mock_location.LocationDetail.Building = uuid.uuid4().hex
        mock_location.LocationDetail.Floor = uuid.uuid4().hex
        mock_location.LocationDetail.Room = uuid.uuid4().hex
        self.mdib.entities.by_node_type.return_value = [mock.MagicMock(states={'state1': mock_location})]

        result = mk_scopes(self.mdib)
        self.assertIn(
            SdcLocation(
                fac=mock_location.LocationDetail.Facility,
                poc=mock_location.LocationDetail.PoC,
                bed=mock_location.LocationDetail.Bed,
                bldng=mock_location.LocationDetail.Building,
                flr=mock_location.LocationDetail.Floor,
                rm=mock_location.LocationDetail.Room,
            ).scope_string,
            result.text[0],
        )

    def test_context_associations(self):
        root = uuid.uuid4().hex
        extension = uuid.uuid4().hex
        # Test with multiple context associations
        mock_state = mock.MagicMock()
        mock_state.ContextAssociation = self.pm_types_associated
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
        # Test device component-based scopes
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

    def test_scope_contains_key_purpose_service_provider(self):
        result = mk_scopes(self.mdib)
        self.assertEqual('sdc.mds.pkp:1.2.840.10004.20701.1.1', result.text[0])
