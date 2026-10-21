from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import operator as operator_module


class DataQualityOperator(BaseOperator):
    """
    Runs a list of parameterized data-quality checks against Redshift.

    Each check is a dict with:
        - check_sql: the SQL to execute (must return a single scalar value)
        - expected_result: the value to compare against
        - comparison: one of '=', '!=', '>', '>=', '<', '<=' (default '=')

    No test is hardcoded in the operator itself; all checks are supplied
    by the caller via the `dq_checks` parameter.
    """

    ui_color = '#89DA59'

    comparisons = {
        '=': operator_module.eq,
        '!=': operator_module.ne,
        '>': operator_module.gt,
        '>=': operator_module.ge,
        '<': operator_module.lt,
        '<=': operator_module.le,
    }

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failing_checks = []

        for check in self.dq_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            comparison = check.get('comparison', '=')
            compare_fn = self.comparisons.get(comparison)

            if compare_fn is None:
                raise ValueError(
                    "Unsupported comparison '{}' in data quality check".format(comparison)
                )

            self.log.info("Running data quality check: {}".format(sql))
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                failing_checks.append((sql, "no results returned"))
                continue

            actual_result = records[0][0]

            if not compare_fn(actual_result, expected_result):
                failing_checks.append(
                    (sql, "actual={} {} expected={} was False".format(
                        actual_result, comparison, expected_result
                    ))
                )
            else:
                self.log.info(
                    "Data quality check passed: {} ({} {} {})".format(
                        sql, actual_result, comparison, expected_result
                    )
                )

        if failing_checks:
            messages = "; ".join(
                "'{}' -> {}".format(sql, reason) for sql, reason in failing_checks
            )
            raise ValueError("Data quality check(s) failed: {}".format(messages))

        self.log.info(
            "All {} data quality checks passed".format(len(self.dq_checks))
        )
