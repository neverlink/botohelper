# BotoHelper
### A small helper library for the AWS Boto3 API
#### Facilitates the use of common AWS endpoints (mainly Athena)

### To-do
Add `BatchGetQueryExecution`

### Prerequisites
- AWS Management Console (if running outside of AWS)
1) Sufficient lambda function permissions /  **OR**
2) AWS Access keys (`aws_access_key_id` & `aws_secret_access_key`), either
    - Set on your local machine with `aws configure` (recommended)
    - Passed as parameters to the class constructor
- Amazon like changing where to find them occasionally - refer to the documentation/google

### Functionality
- View Glue Catalogs
- View Databases in Glue Catalogs
- Launch an Athena query
- Wait for the result of an athena query (synchronously)
    - Get the result as a matrix, dictionary or string (a convenient, human-readable table)
- Upload JSON to S3 (old and not used, needs some tweaking)
