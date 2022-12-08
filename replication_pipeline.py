"""
Only showing the steps in the replication DAG in this example project
"""

import os
import pendulum

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator

from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator


def check_data_unloaded(unload_count):
    if unload_count is None or unload_count < 1:
        return True
    return False


def hash_comparison(hash1, hash2, true_task, false_task):
    if hash1 == hash2:
        return true_task
    return false_task


with DAG(
        dag_id="redshift-replication-pipeline",
        schedule_interval="@daily",
        start_date=pendulum.parse('2015-01-01'),  # the dag schedules run for every schedule interval after this date
) as replication_dag:
    query_params = {
        'selection_date': '{{ ds }}',
        'table_schema': '',
        'table_name': 'transactions'
    }

    # unload data to s3
    unload_data = RedshiftToS3Operator(
        task_id="unload_source_redshift_table",
        sql=open('sql/template_select_by_day_from_table.sql', 'r').read(),
        params=query_params,
        s3_bucket="my_unload_bucket",
        s3_key="unload/" + query_params['table_name']
    )

    # shortcircuit if no unload
    check_unload = ShortCircuitOperator(
        task_id='shortcircuit_if_no_unload',
        python_callable=check_data_unloaded(unload_data)
    )

    # delete existing entries
    clear_data = RedshiftSQLOperator(
        task_id='delete_existing_records',
        sql=f'delete from {query_params["table_schema"]}.{query_params["table_name"]} where business_date = query_params["selection_date"]'
    )

    # copy the data from s3
    copy_data = S3ToRedshiftOperator(
        task_id="copy_to_destination_table",
        s3_bucket='my_unload_bucket',
        s3_key="unload/" + query_params['table_name'],
        schema='destination_schema',
        table='destination_datable'
    )

    # hash the source and the destination
    hash_source = RedshiftSQLOperator(
        task_id="hash_source",
        # I can share the actual hashing method. It is much more complicated than this.
        sql="select shash from (select sha2(a, 256) from destination_schema.destination_table order by a)"
    )

    hash_destination = RedshiftSQLOperator(
        task_id="hash_destination",
        # I can share the actual hashing method. It is much more complicated than this.
        sql="select shash from (select sha2(a, 256) from destination_schema.destination_table order by a)"
    )

    # compare hashes
    compare_hashes = BranchPythonOperator(
        task_id="compare_hashes",
        python_callable=lambda hash_src, hash_dest: hash_source == hash_destination,
        op_kwargs={
            "hash_src": hash_source,
            "hash_dest": hash_destination,
            "task_if_true": "success_message",
            "task_if_false": "failure_message"
        }
    )

    # alerts triggerred via sns are then picked up by Opsgenie via a lambda triggered by publish event
    success_alert = SnsPublishOperator(
        task_id="success_alert",
        message="replication successful for " + query_params['table_schema'] + '.' + query_params['table_name'],
        subject="redshfit replicator",
        target_arn="mytargetarn_pulled_from_config"
    )

    fail_alert = SnsPublishOperator(
        task_id="fail_alert",
        message="replication failed for " + query_params['table_schema'] + '.' + query_params['table_name'],
        subject="redshift replicator",
        target_arn="mytargetarn_pulled_from_config"
    )


    unload_data >> check_unload >> clear_data >> copy_data >> (hash_source, hash_destination) >> compare_hashes >> (success_alert, fail_alert)

