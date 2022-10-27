import requests
import urllib
import json
from requests.auth import HTTPBasicAuth
import uuid


class RedRoseAPIError(Exception):
    """An exception class for the client"""


def http_build_query(data):
    parents = list()
    pairs = dict()

    def renderKey(parents):
        depth, outStr = 0, ''
        for x in parents:
            s = "[%s]" if depth > 0 or isinstance(x, int) else "%s"
            outStr += s % str(x)
            depth += 1
        return outStr

    def r_urlencode(data):
        if isinstance(data, list) or isinstance(data, tuple):
            for i in range(len(data)):
                parents.append(i)
                r_urlencode(data[i])
                parents.pop()
        elif isinstance(data, dict):
            for key, value in data.items():
                parents.append(key)
                r_urlencode(value)
                parents.pop()
        else:
            pairs[renderKey(parents)] = str(data)

        return pairs
    return urllib.parse.urlencode(r_urlencode(data))


class RedRoseAPI:

    url_path = '/externalapi/'

    def __init__(self, url, api_user, api_key, module):
        self.url = url
        self.api_user = api_user
        self.api_key = api_key
        self.module = module
        self.status_code = None

    def request(self, method, action, params=None, files=None):

        kwargs = {
            'url': self.normalize_url(action),
            'auth': (self.api_user, self.api_key),
        }

        if files is not None:
            kwargs['files'] = [('keyValuePair', ('keyValuePair', json.dumps(files), 'application/json'))]
        if params is not None:
            kwargs['url'] = kwargs['url'] + '?' + http_build_query(params)

        response = requests.request(method, **kwargs)

        self.status_code = response.status_code

        if self.status_code != 200:
            reason = self.parse_reason(response.headers)
            raise RedRoseAPIError(f'Wrong request, status code is {response.status_code}, reason is {reason}')

        data = response.content
        if not data:
            raise RedRoseAPIError('Wrong request, content response is empty')

        return response.json()

    def normalize_url(self, action):
        return self.url + self.url_path + 'modules/' + self.module + '/' + action

    @staticmethod
    def parse_reason(headers):
        if 'X-Status-Reason' not in headers:
            return 'Unknown Error'

        return headers['X-Status-Reason']


class RedRosePaymentsAPI:

    def __init__(self, host_name=None, user_name=None, password=None):
        self.host_name = host_name
        self.basic_auth = HTTPBasicAuth(user_name, password)

    def update_beneficiary_list_from_excel(self, comment, filename, file_path):
        # 1. to create a new group in the system from excel file
        response = self._post(
            '/api/beneficiaryList/updateBeneficiaryListFromExcel',
            params={
                'pipeSeparatedMatcherFields': 'm.iqId',
                'headerRowIndex': '1',
                'ignoreDuplicateNames': 'true',
                'comment': comment
            },
            payload={},
            files=RedRosePaymentsAPI._files_excel('file', filename, file_path)
        )
        if not response:
            return str(uuid.uuid4()).lower()
        elif response.status_code == 200:
            return response.json()
        else:
            raise Exception('update_beneficiary_list_from_excel failed, status code: ' + str(response.status_code))

    def get_excel_import_status(self, excel_import_id):
        # 2. check if template file is processed
        # 6. check uploaded distribution file is processed
        response = self._get(
            '/api/bulk/getExcelImportStatus/' + excel_import_id,
            params=None
        )
        if not response:
            return {
                'status': 'SUCCEEDED'
            }
        elif response.status_code == 200:
            return response.json()
        else:
            raise Exception('get_excel_import_status failed, status code: ' + str(response.status_code))

    def download_individual_distribution_excel(self, beneficiary_group_id, activity_id, local_file_name):
        # 4. after creating the group, get the group id and use this to download the file to be filled in
        # activity id is fixed
        return self._download_excel_file(
            '/api/activity/downloadIndividualDistributionExcel',
            {
                'beneficiaryGroupId': beneficiary_group_id,
                'activityId': activity_id
            },
            'xlsx/' + local_file_name
        )

    def upload_individual_distribution_excel(self, filename, file_path, activity_id):
        # 5. after filling the individual amounts, upload it back into the system
        response = self._post(
            '/api/activity/uploadIndividualDistributionExcel',
            params={
                'activityId': activity_id,
                'approveProposalsAutomatically': 'true'
            },
            payload={},
            files=RedRosePaymentsAPI._files_excel('distFile', filename, file_path) if self.host_name else None
        )
        if not response:
            return str(uuid.uuid4()).lower()
        elif response.status_code == 200:
            return response.json()
        else:
            raise Exception('update_beneficiary_list_from_excel failed, status code: ' + str(response.status_code))

    def get_beneficiary_group(self, beneficiary_group_name):
        # 3. check if group is created (after processing)
        response = self._get(
            '/api/beneficiaryGroupList/list',
            params={
                'ignoreDeletedMarkerColumn': 'false',
                'filter[0][column]': 'name',
                'filter[0][value]': beneficiary_group_name
            }
        )
        if not response:
            return None
        elif response.status_code == 200:
            return response.json()
        else:
            raise Exception('get_beneficiary_group failed, status code: ' + str(response.status_code))

    @staticmethod
    def _files_excel(file_param, filename, file_path):
        return [
            (file_param, (
               filename,
               open(file_path, 'rb'),
               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            ))
        ]

    def _download_excel_file(self, url, params, local_filename):
        if not self.host_name:
            return None
        with requests.get(
                'https://' + self.host_name + url, stream=True, params=params, auth=self.basic_auth
        ) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename

    # @with_response_json
    def _get(self, url, params):
        if not self.host_name:
            return None
        return requests.request(
            "GET", 'https://' + self.host_name + url, params=params, auth=self.basic_auth
        )

    def _post(self, url, params, payload, files):
        if not self.host_name:
            return None
        return requests.request(
            "POST", 'https://' + self.host_name + url, params=params, data=payload, files=files,
            auth=self.basic_auth
        )