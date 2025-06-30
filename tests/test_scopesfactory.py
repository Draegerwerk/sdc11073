"""Unittest for scopes factory module."""

from __future__ import annotations

import urllib.parse
import uuid
from unittest import mock

import pytest

from sdc11073.location import SdcLocation
from sdc11073.mdib import statecontainers
from sdc11073.provider.scopesfactory import BICEPS_URI_UNK, KEY_PURPOSE_SERVICE_PROVIDER, mk_scopes
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.wsd_types import ScopesType


@pytest.fixture
def mdib() -> mock.MagicMock:
    mdib = mock.MagicMock()
    mdib.data_model.pm_types.ContextAssociation.ASSOCIATED = pm_types.ContextAssociation.ASSOCIATED
    mdib.data_model.pm_names.LocationContextDescriptor = 'LocationContextDescriptor'
    mdib.data_model.pm_names.OperatorContextDescriptor = 'OperatorContextDescriptor'
    mdib.data_model.pm_names.EnsembleContextDescriptor = 'EnsembleContextDescriptor'
    mdib.data_model.pm_names.WorkflowContextDescriptor = 'WorkflowContextDescriptor'
    mdib.data_model.pm_names.MeansContextDescriptor = 'MeansContextDescriptor'
    mdib.data_model.pm_names.MdsDescriptor = 'MdsDescriptor'
    return mdib


def test_no_associated_locations(mdib: mock.MagicMock):
    """Test when there are no associated locations."""
    result = mk_scopes(mdib)
    assert isinstance(result, ScopesType)
    assert len(result.text) == 1  # Only the default key purpose scope
    assert result.text[0] == KEY_PURPOSE_SERVICE_PROVIDER


_rnd_identifier = uuid.uuid4().hex


@pytest.mark.parametrize(
    ('identifier', 'expected'),
    [
        (_rnd_identifier, _rnd_identifier),
        (None, BICEPS_URI_UNK),
        ('', BICEPS_URI_UNK),
        (' ', ' '),
        ('/', '/'),
        ('%', '%'),
        ('&', '&'),
    ],
)
def test_fallback_instance_algorithm(mdib: mock.MagicMock, identifier: str | None, expected: str):
    """Test with a single associated location."""
    sdc_location = SdcLocation(
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
    loc_state.Identification[0].Root = identifier  # overwrite root with the test value
    mdib.entities.by_node_type.side_effect = (
        lambda nodetype: [
            mock.MagicMock(states={uuid.uuid4().hex: loc_state}),
        ]
        if nodetype == 'LocationContextDescriptor'
        else []
    )
    result = mk_scopes(mdib)
    assert (
        result.text[0][: len(f'sdc.ctxt.loc:/{urllib.parse.quote(expected, safe="")}')]
        == f'sdc.ctxt.loc:/{urllib.parse.quote(identifier or BICEPS_URI_UNK, safe="")}'
    )


def test_context_associations(mdib: mock.MagicMock):
    root = uuid.uuid4().hex
    extension = uuid.uuid4().hex

    def _node_type_side_effect(nodetype):
        if nodetype == 'OperatorContextDescriptor':
            return [
                mock.MagicMock(
                    states={
                        'state1': mock.MagicMock(
                            ContextAssociation=pm_types.ContextAssociation.ASSOCIATED,
                            Identification=[mock.MagicMock(Root=root, Extension=extension)],
                        )
                    }
                )
            ]
        if nodetype != 'MdsDescriptor':
            return [
                mock.MagicMock(
                    states={
                        uuid.uuid4(): mock.MagicMock(
                            ContextAssociation=pm_types.ContextAssociation.DISASSOCIATED,
                            Identification=[mock.MagicMock(Root=uuid.uuid4().hex, Extension=uuid.uuid4().hex)],
                        )
                    }
                )
            ]
        return []

    mdib.entities.by_node_type.side_effect = _node_type_side_effect

    result = mk_scopes(mdib)
    assert len(result.text) == 2
    assert result.text[0] == f'sdc.ctxt.opr:/{root}/{extension}'


def test_device_component_based_scopes(mdib: mock.MagicMock):
    """Test device component-based scopes."""
    mock_entity = mock.MagicMock()
    mock_entity.descriptor.Type.CodingSystem = uuid.uuid4().hex
    mock_entity.descriptor.Type.CodingSystemVersion = uuid.uuid4().hex
    mock_entity.descriptor.Type.Code = uuid.uuid4().hex
    mdib.entities.by_node_type.side_effect = (
        lambda nodetype: [
            mock_entity,
        ]
        if nodetype == 'MdsDescriptor'
        else []
    )

    result = mk_scopes(mdib)
    assert (
        result.text[0]
        == f'sdc.cdc.type:/{mock_entity.descriptor.Type.CodingSystem}/{mock_entity.descriptor.Type.CodingSystemVersion}/{mock_entity.descriptor.Type.Code}'
    )


def test_raise_error_if_no_identification_element(mdib: mock.MagicMock):
    """Test that an error is raised if a location state has no Identification."""
    loc_state = statecontainers.LocationContextStateContainer(
        mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
        uuid.uuid4().hex,
    )
    loc_state.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
    mdib.entities.by_node_type.return_value = [mock.MagicMock(states={uuid.uuid4().hex: loc_state})]

    with pytest.raises(
        ValueError,
        match=f'State {loc_state.Handle} of type {mdib.data_model.pm_names.LocationContextDescriptor} has no '
        f'Identification element',
    ):
        mk_scopes(mdib)


def test_raise_error_if_no_location_detail_element(mdib: mock.MagicMock):
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
    mdib.entities.by_node_type.return_value = [mock.MagicMock(states={uuid.uuid4().hex: loc_state})]

    with pytest.raises(
        ValueError,
        match=f'State {loc_state.Handle} of type {mdib.data_model.pm_names.LocationContextDescriptor} has no '
        f'LocationDetail element',
    ):
        mk_scopes(mdib)
