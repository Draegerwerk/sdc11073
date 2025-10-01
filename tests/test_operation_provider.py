"""Tests for operation provider role implementation."""

from collections.abc import Iterable
from types import SimpleNamespace

import pytest

from sdc11073 import xml_utils
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.provider.operations import ActivateOperation
from sdc11073.roles.operationprovider import OperationProvider
from sdc11073.xml_types import msg_types, pm_types


class FakeState:
    def __init__(self):
        self.MetricValue = None

    def mk_metric_value(self):
        class MV:
            pass

        self.MetricValue = MV()


class FakeMgr:
    def __init__(self, state: FakeState):
        self._state = state

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return False

    def get_state(self, _handle: str) -> FakeState:
        return self._state


class FakeMdib:
    def __init__(self):
        self._state = FakeState()
        self.data_model = SimpleNamespace(msg_types=msg_types)

    def metric_state_transaction(self, set_determination_time: bool = True) -> FakeMgr:  # noqa: ARG002
        return FakeMgr(self._state)


class FakeActivateDescriptor:
    def __init__(self, handle: str, target: str, arg_qnames: Iterable[xml_utils.QName]):
        self.Handle = handle
        self.OperationTarget = target
        self.NODETYPE = ns_hlp.PM.tag('ActivateOperationDescriptor')
        self.Argument = [pm_types.ActivateOperationDescriptorArgument(arg_name=None, arg=q) for q in arg_qnames]


def _make_activate_args(values: Iterable[str]) -> msg_types.Activate:
    act = msg_types.Activate()
    for v in values:
        a = msg_types.Argument()
        a.ArgValue = str(v)
        act.Argument.append(a)
    return act


def test_make_operation_instance_handle_match():
    prov = OperationProvider(FakeMdib(), log_prefix='t')
    desc = FakeActivateDescriptor('activate_1.sco.mds_0', 'metric0', [])
    op = prov.make_operation_instance(desc, lambda _: ActivateOperation)
    assert isinstance(op, ActivateOperation)
    assert op.operation_target_handle == 'metric0'


def test_make_operation_instance_non_matching_handle_returns_none():
    prov = OperationProvider(FakeMdib(), log_prefix='t')
    desc = FakeActivateDescriptor('other.sco.handle', 'metric0', [])
    assert prov.make_operation_instance(desc, lambda _: ActivateOperation) is None


def test_handle_plugathon_activate_success_and_errors():
    prov = OperationProvider(FakeMdib(), log_prefix='t')

    # success: string, decimal, anyURI
    arg_types = [
        ns_hlp.XSD.tag('string'),
        ns_hlp.XSD.tag('decimal'),
        ns_hlp.XSD.tag('anyURI'),
    ]
    desc = FakeActivateDescriptor('activate_1.sco.mds_0', 'metric0', arg_types)
    op_instance = SimpleNamespace(descriptor_container=desc, operation_target_handle='metric0')
    req = _make_activate_args(['abc', '1.23', 'http://x'])
    params = SimpleNamespace(operation_instance=op_instance, operation_request=req)
    result = prov._handle_plugathon_activate(params)
    assert result.invocation_state == msg_types.InvocationState.FINISHED
    assert prov._mdib._state.MetricValue.Value == 'abc1.23http://x'

    # wrong number of args -> ValueError
    desc_err = FakeActivateDescriptor('activate_1.sco.mds_0', 'metric0', arg_types[:2])
    params_err = SimpleNamespace(
        operation_instance=SimpleNamespace(descriptor_container=desc_err),
        operation_request=req,
    )
    with pytest.raises(
        ValueError,
        match=f'Expected {len(params_err.operation_instance.descriptor_container.Argument)} arguments, '
        f'got {len(params_err.operation_request.Argument)}',
    ):
        prov._handle_plugathon_activate(params_err)

    # unsupported type -> NotImplementedError
    bad_type = ns_hlp.XSD.tag('integer')
    desc_bad = FakeActivateDescriptor('activate_1.sco.mds_0', 'metric0', [bad_type])
    bad_req = _make_activate_args(['1'])
    with pytest.raises(NotImplementedError):
        prov._handle_plugathon_activate(
            SimpleNamespace(
                operation_instance=SimpleNamespace(descriptor_container=desc_bad),
                operation_request=bad_req,
            ),
        )
