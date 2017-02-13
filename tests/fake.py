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


class FakeSnakeBiteClientException(Exception):
    pass


class FakeSnakeBiteClient(object):

    def __init__(self):
        self.started = True

    def ls(self, path, include_toplevel=False):
        """
        the fake snakebite client
        :param path: the array of path to test
        :param include_toplevel: to return the toplevel directory info
        :return: a list for path for the matching queries
        """
        if path[0] == '/datadirectory/empty_directory' and not include_toplevel:
            return []
        elif path[0] == '/datadirectory/datafile':
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/datafile'}]
        elif path[0] == '/datadirectory/empty_directory' and include_toplevel:
            return [
                {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0, 'block_replication': 0,
                 'modification_time': 1481132141540, 'length': 0, 'blocksize': 0, 'owner': u'hdfs',
                 'path': '/datadirectory/empty_directory'}]
        elif path[0] == '/datadirectory/not_empty_directory' and include_toplevel:
            return [
                {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0, 'block_replication': 0,
                 'modification_time': 1481132141540, 'length': 0, 'blocksize': 0, 'owner': u'hdfs',
                 'path': '/datadirectory/empty_directory'},
                {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                 'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                 'owner': u'hdfs', 'path': '/datadirectory/not_empty_directory/test_file'}]
        elif path[0] == '/datadirectory/not_empty_directory':
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/not_empty_directory/test_file'}]
        elif path[0] == '/datadirectory/not_existing_file_or_directory':
            raise FakeSnakeBiteClientException
        elif path[0] == '/datadirectory/regex_dir':
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test1file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test2file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test3file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/copying_file_1.txt._COPYING_'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/copying_file_3.txt.sftp'}
                    ]
        else:
            raise FakeSnakeBiteClientException


class FakeHDFSHook(object):
    def __init__(self, conn_id=None):
        self.conn_id = conn_id

    def get_conn(self):
        client = FakeSnakeBiteClient()
        return client
