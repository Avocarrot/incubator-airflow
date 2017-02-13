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
from mock import Mock, patch

from airflow.contrib.sensors.datadog_sensor import DatadogSensor


at_least_one_event = [{'alert_type': 'info',
                       'comments': [],
                       'date_happened': 1419436860,
                       'device_name': None,
                       'host': None,
                       'id': 2603387619536318140,
                       'is_aggregate': False,
                       'priority': 'normal',
                       'resource': '/api/v1/events/2603387619536318140',
                       'source': 'My Apps',
                       'tags': ['application:web', 'version:1'],
                       'text': 'And let me tell you all about it here!',
                       'title': 'Something big happened!',
                       'url': '/event/jump_to?event_id=2603387619536318140'},
                      {'alert_type': 'info',
                       'comments': [],
                       'date_happened': 1419436865,
                       'device_name': None,
                       'host': None,
                       'id': 2603387619536318141,
                       'is_aggregate': False,
                       'priority': 'normal',
                       'resource': '/api/v1/events/2603387619536318141',
                       'source': 'My Apps',
                       'tags': ['application:web', 'version:1'],
                       'text': 'And let me tell you all about it here!',
                       'title': 'Something big happened!',
                       'url': '/event/jump_to?event_id=2603387619536318141'}]

zero_events = []

mock_connection = Mock(extra_dejson={'api_key': 'foo', 'app_key': 'bar'})
patch_connection = patch('airflow.contrib.hooks.datadog_hook.DatadogHook.get_connection',
                         return_value=mock_connection)


class TestDatadogSensor(unittest.TestCase):

    @patch('airflow.contrib.sensors.datadog_sensor.api.Event.query',
           return_value=at_least_one_event)
    @patch_connection
    def test_sensor_ok(self, *_):
        sensor = DatadogSensor(
            task_id='test_datadog',
            datadog_conn_id='datadog_default',
            from_seconds_ago=3600,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None)

        self.assertTrue(sensor.poke({}))

    @patch('airflow.contrib.sensors.datadog_sensor.api.Event.query',
           return_value=[])
    @patch_connection
    def test_sensor_fail(self, *_):
        sensor = DatadogSensor(
            task_id='test_datadog',
            datadog_conn_id='datadog_default',
            from_seconds_ago=0,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None)

        self.assertFalse(sensor.poke({}))

if __name__ == '__main__':
    unittest.main()
