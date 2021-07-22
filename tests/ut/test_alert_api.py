from datetime import datetime

from ph.alert_api import unify_time_format, remove_fields


def test_unify_time_format():
    fields = ['start_time', 'end_time', 'any_time']
    tests = [
        ('2020-12-21 08:25:00', 1608567900),
        ('2021-02-03 08:10:00.12', 1612368600),
        (1615431612, 1615431612),
        ('2021-03-22T16:34:16.228614279Z', 1615431612),
        ('2021-03-22T16:38:45.228614279', 1624420487),
        ('something not datetime', int(datetime.utcnow().timestamp()))
    ]

    alerts = []
    for t, res in tests:
        alerts.append(unify_time_format({'record': {'start_time': t, 'end_time': t, 'd': 'dd'}}, fields))
        print(t, alerts[-1])
        print()

    # all values should be unique
    assert len(tests) == len(set([a['record']['start_time'] for a in alerts]))
    assert len(tests) == len(set([a['record']['end_time'] for a in alerts]))


def test_remove_fields():
    # ((alert, removed_fields), expected_result)
    tests = [
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, []),
         {'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}),
        (({}, ['start_time']), {}),
        (({'record': {}}, ['start_time']), {'record': {}}),
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['start_time']),
         {'record': {'end_time': 234, 'd': 'dd'}}),
        (
        ({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['start_time', 'end_time', 'd']), {'record': {}}),
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['aa', 'bb']),
         {'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}),
    ]

    for (alert, removed_fields), expected in tests:
        assert expected == remove_fields(alert, removed_fields)


def test_remove_fields_configure():
    # ((alert, removed_fields), expected_result)
    tests = [
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, []),
         {'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}),
        (({}, ['start_time']), {}),
        (({'record': {}}, ['start_time']), {'record': {}}),
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['start_time']),
         {'record': {'end_time': 234, 'd': 'dd'}}),
        (
        ({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['start_time', 'end_time', 'd']), {'record': {}}),
        (({'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}, ['aa', 'bb']),
         {'record': {'start_time': 234, 'end_time': 234, 'd': 'dd'}}),
    ]

    for (alert, removed_fields), expected in tests:
        assert expected == remove_fields(alert, removed_fields)
