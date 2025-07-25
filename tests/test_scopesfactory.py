"""Unittest for scopes factory module."""

from __future__ import annotations

import uuid
from unittest import mock

import pytest

from sdc11073.location import SdcLocation
from sdc11073.mdib import statecontainers
from sdc11073.provider.scopesfactory import (
    BICEPS_URI_UNK,
    KEY_PURPOSE_SERVICE_PROVIDER,
    _query_from_location_state,
    mk_scopes,
)
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


@pytest.mark.parametrize(
    ('identifier', 'expected'),
    [
        ('some_string', 'sdc.ctxt.loc:/some_string'),
        (None, f'sdc.ctxt.loc:/{BICEPS_URI_UNK}'),
        ('', 'sdc.ctxt.loc:/'),
        (' ', 'sdc.ctxt.loc:/%20'),
        ('/', 'sdc.ctxt.loc:/%2F'),
        ('%', 'sdc.ctxt.loc:/%25'),
        ('&', 'sdc.ctxt.loc:/%26'),
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
    loc_state.LocationDetail = None
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
    assert result.text[0][: len(expected)] == expected


def test_context_associations(mdib: mock.MagicMock):
    root = uuid.uuid4().hex
    extension = uuid.uuid4().hex

    def _node_type_side_effect(nodetype: str) -> list[mock.MagicMock]:
        if nodetype == 'OperatorContextDescriptor':
            return [
                mock.MagicMock(
                    states={
                        'state1': mock.MagicMock(
                            ContextAssociation=pm_types.ContextAssociation.ASSOCIATED,
                            Identification=[mock.MagicMock(Root=root, Extension=extension)],
                        ),
                    },
                ),
            ]
        if nodetype != 'MdsDescriptor':
            return [
                mock.MagicMock(
                    states={
                        uuid.uuid4(): mock.MagicMock(
                            ContextAssociation=pm_types.ContextAssociation.DISASSOCIATED,
                            Identification=[mock.MagicMock(Root=uuid.uuid4().hex, Extension=uuid.uuid4().hex)],
                        ),
                    },
                ),
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
        result.text[0] == f'sdc.cdc.type:/'
        f'{mock_entity.descriptor.Type.CodingSystem}/'
        f'{mock_entity.descriptor.Type.CodingSystemVersion}/'
        f'{mock_entity.descriptor.Type.Code}'
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


def test_raise_error_if_empty_location():
    """Test that an error is raised if a location state has empty LocationDetail."""
    loc_state = statecontainers.LocationContextStateContainer(
        mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
        uuid.uuid4().hex,
    )
    with pytest.raises(
        ValueError,
        match='Location extension segment is empty, at least one element must be set',
    ):
        loc_state.update_from_sdc_location(SdcLocation())


@pytest.mark.parametrize('url_element', ['fac', 'bldng', 'flr', 'poc', 'rm', 'bed'])
@pytest.mark.parametrize(
    ('loc_value', 'expected'),
    [
        ('some_string', 'some_string'),
        (None, None),  # expected value does not matter for None
        ('', ''),
        (' ', '%20'),
        ('/', '%2F'),
        ('%', '%25'),
        ('&', '%26'),
    ],
)
def test_query_from_location_state(url_element: str, loc_value: str | None, expected: str):
    """Test the query_from_location_state function."""
    loc_state = statecontainers.LocationContextStateContainer(
        mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
        uuid.uuid4().hex,
    )
    loc = SdcLocation(
        fac=uuid.uuid4().hex,
        poc=uuid.uuid4().hex,
        bed=uuid.uuid4().hex,
        bldng=uuid.uuid4().hex,
        flr=uuid.uuid4().hex,
        rm=uuid.uuid4().hex,
    )
    setattr(loc, url_element, loc_value)
    loc_state.update_from_sdc_location(loc)
    query = _query_from_location_state(loc_state)
    if loc_value is None:
        assert f'{url_element}=' not in query
    else:
        assert f'{url_element}={expected}' in query


def test_query_from_location_state_with_empty_details():
    """Test the query_from_location_state function."""
    loc_state = statecontainers.LocationContextStateContainer(
        mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
        uuid.uuid4().hex,
    )
    query = _query_from_location_state(loc_state)
    assert query == ''

    loc_state.LocationDetail = None
    query = _query_from_location_state(loc_state)
    assert query == ''


def test_location_detail_query_with_special_char():
    """Test the query_from_location_state directly with LocationDetail function."""
    location_detail = pm_types.LocationDetail(
        poc='poc1',
        room='room1',
        bed='bed1',
        facility='facility1&',
        building='building1',
        floor='floor1',
    )
    loc_state = statecontainers.LocationContextStateContainer(
        mock.MagicMock(Handle=uuid.uuid4().hex, DescriptorVersion=uuid.uuid4().int),
        uuid.uuid4().hex,
    )
    loc_state.LocationDetail = location_detail
    actual_uri = _query_from_location_state(loc_state)
    expected_uri = 'fac=facility1%26&bldng=building1&flr=floor1&poc=poc1&rm=room1&bed=bed1'
    assert actual_uri == expected_uri
