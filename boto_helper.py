import time
import json
import boto3

class BotoHelper:
    def __init__(self, key_id=None, key_secret=None, verify_ssl=True):
        self.session = boto3.Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=key_secret,
            region_name='eu-central-1'
        )
        self.athena = self.session.client('athena', verify=verify_ssl)
        self.s3 = self.session.resource('s3', verify=verify_ssl)

    # Returns a list of available catalogs (data stores)
    def fetch_catalogs(self):
        return self.athena.list_data_catalogs()['DataCatalogsSummary']

    # Returns a list of queryable databases within a catalog
    def fetch_databases(self, catalog_name):
        return self.athena.list_databases(CatalogName=catalog_name)['DatabaseList']

    # Provides details about the current state of a query. See docs for details
    def fetch_query_status(self, query_id):
        return self.athena.get_query_execution(QueryExecutionId=query_id)

    # Extracts the current execution status of the query
    # Outputs QUEUED/RUNNING/SUCCEEDED/FAILED/CANCELLED
    def fetch_query_state(self, query_id):
        return self.fetch_query_status(query_id)['QueryExecution']['Status']['State']

    # Converts the JSON response from Athena into a nested array
    def convert_to_matrix(self, query_result):
        matrix = [row['Data'] for row in query_result['ResultSet']['Rows']]
        for x, _ in enumerate(matrix):
            for y, col in enumerate(matrix[x]):
                if len(col) == 0:
                    matrix[x][y] = 'NULL'
                else:
                    matrix[x][y] = matrix[x][y]['VarCharValue']
        return matrix

    # Converts the matrix into a dictionary
    # Each key is a column and the value is the corresponding row
    # e.g result[1][5] -> result['impressions'][5]
    def convert_to_dict(self, matrix):
        result = {}
        # Set a key for each column
        for col_name in matrix[0]:
            result[col_name] = []

        # Split each row into the arrays defined above
        for row in matrix[1:]:
            for index, value in enumerate(row):
                result[matrix[0][index]].append(value)
        return result

    # Returns an easy-to-read string representation of a matrix
    def convert_to_table(self, matrix):
        table = ''
        col_lengths = [0 for x in matrix[0]]

        # Fills the above array with the maximum lengths for each column
        for row_index, _ in enumerate(matrix):
            for col_index, col in enumerate(matrix[row_index]):
                if len(col) > col_lengths[col_index]:
                    col_lengths[col_index] = len(col)

        # Separator
        separator = '|'
        for col_length in col_lengths:
            separator += '-' * (col_length + 2) + '|'
        separator += '\n'

        # Delimits columns with pipes and fills in any gaps with space
        for row_index, _ in enumerate(matrix):
            if row_index == 1:
                table += separator

            table += '|'
            for col_index, col in enumerate(matrix[row_index]):
                table += ' ' + col + ' ' * (col_lengths[col_index] - len(col)) + ' |'
            table += '\n'
        
        table = separator + table + separator
        return table.strip()

    # Queues an Athena query and returns the execution ID
    def launch_query(self, database, query):
        query_execution = self.athena.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {
                'Database': database
            },
            ResultConfiguration = {
                'OutputLocation': 's3://boris-experimenting/results-dump',
            }
        )
        return query_execution['QueryExecutionId']

    # Returns the results of a query in a particular format: dict, list or str
    def fetch_query_results(self, query_id, desired_return):
        query_result = self.athena.get_query_results(QueryExecutionId=query_id)
        matrix = self.convert_to_matrix(query_result)

        if desired_return == list:
            return matrix
        elif desired_return == dict:
            return self.convert_to_dict(matrix)
        elif desired_return == str:
            return self.convert_to_table(matrix)
        else:
            raise Exception('Invalid reutrn type provided!')

    # This is a blocking method
    def await_query_result(self, database, query, desired_return, verbose=False):
        query_id = self.launch_query(database, query)
        query_state = self.fetch_query_state(query_id)
        
        start_time = time.time()

        while query_state == 'QUEUED' or query_state == 'RUNNING':
            query_state = self.fetch_query_state(query_id)
            
            elapsed = time.time() - start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            erase = '\x1b[1A\x1b[2K'
            
            if verbose:
                print(erase + f'Elapsed: {minutes}:{seconds:02} Query state: {query_state}')
            time.sleep(0.5)

        if query_state == 'SUCCEEDED':
            return self.fetch_query_results(query_id, desired_return)
        else:
            return None

    # Uploads the parameter as a text file to an S3 bucket
    def export(self, result):
        s3_obj = self.s3.Object('boris-experimenting', 'latest_result.json')

        if type(result) is not str:
            result = json.dumps(result, indent=4)

        upload = s3_obj.put(Body=result)
        res = upload.get('ResponseMetadata')

        # Might return an incorrect message if uploading anything large
        if res.get('HTTPStatusCode') == 200:
            print('File Uploaded Successfully')
        else:
            print('File Upload Failed!')
