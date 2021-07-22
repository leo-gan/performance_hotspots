import os

from ph.globals import jobs, _jobs


def test_reset_jobs():
    all_job_names = {job.name for job in _jobs}
    tests = [None, '', 'port_scan', 'port_scan,ip_sweep', 'port_scan,bytes_out', 'port_scan,ip_sweep,bytes_out',
             'port_scanW,ip_sweepW,bytes_outW', 'port_scanW,ip_sweep,bytes_outW',]

    # save the current value of AD_DISABLED_JOBS:
    old_val = os.getenv('AD_DISABLED_JOBS', '')

    # tests:
    for disabled in tests:
        os.environ['AD_DISABLED_JOBS'] = disabled if disabled else ''
        job_names = {job.name for job in jobs()}
        expected = all_job_names - set(disabled.split(',') if disabled else [])
        assert expected == job_names

    # restore the current value of AD_DISABLED_JOBS. The side-effect is the env var created,
    # even it didn't exist before. In this case it has a '' values, which does not harm.
    os.environ['AD_DISABLED_JOBS'] = old_val
    assert os.getenv('AD_DISABLED_JOBS', '') == old_val
