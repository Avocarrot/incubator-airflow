# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

TERMINATE_SUCCESS_RETURN = {
    'ResponseMetadata': {
        'HTTPStatusCode': 200
    }
}


class TestEmrTerminateJobFlowOperator(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        # Mock out the emr_client (moto has incorrect response)
        mock_emr_client = MagicMock()
        mock_emr_client.terminate_job_flows.return_value = TERMINATE_SUCCESS_RETURN

        # Mock out the emr_client creator
        self.boto3_client_mock = MagicMock(return_value=mock_emr_client)


    def test_execute_terminates_the_job_flow_and_does_not_error(self):
        with patch('boto3.client', self.boto3_client_mock):

            operator = EmrTerminateJobFlowOperator(
                task_id='test_task',
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default'
            )

            operator.execute(None)