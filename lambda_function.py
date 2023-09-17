import boto3
import zipfile
import io
import subprocess
import os
import requests
import json

s3_client = boto3.client('s3')
codecommit_client = boto3.client('codecommit')
s3_bucket = os.environ.get('UploadBucket')
API_ENDPOINT = os.environ.get("AUTHORIZER_ENDPOINT_URL")

def push_to_codecommit(repo_name, branch_name, unzipped_content):
    """
    Deletes old files from the latest commit and pushes unzipped content to a CodeCommit repository in a single commit.

    Args:
    - repo_name: The name of the CodeCommit repository.
    - branch_name: The name of the branch to which the content should be pushed.
    - unzipped_content: A dictionary with file paths as keys and file content as values.
    """

    # First, get the latest commit ID for the branch.
    branch_info = codecommit_client.get_branch(repositoryName=repo_name, branchName=branch_name)
    latest_commit_id = branch_info['branch']['commitId']

    # Get the list of files in the root of the repository.
    folder_contents = codecommit_client.get_folder(repositoryName=repo_name, folderPath='/')
    existing_files = [entry['absolutePath'] for entry in folder_contents['files']]

    # Prepare the list of changes for putFiles and deleteFiles.
    put_changes = []
    delete_changes = []

    # Mark files for deletion if they aren't in the new content.
    for file_path in existing_files:
        if file_path not in unzipped_content:
            delete_changes.append({
                'filePath': file_path
            })

    # Add or update files from the unzipped content.
    for file_path, content in unzipped_content.items():
        # Ensure the content is not empty
        if content:
            put_changes.append({
                'filePath': file_path,
                'fileMode': 'NORMAL',
                'fileContent': content.decode("utf-8")  # Assuming the content is bytes; convert to string.
            })

    # Create a single commit with all the changes.
    response = codecommit_client.create_commit(
        repositoryName=repo_name,
        branchName=branch_name,
        parentCommitId=latest_commit_id,
        authorName='Lambda Function',
        email='lambda@example.com',
        commitMessage='Updated via Lambda',
        keepEmptyFolders=True,
        putFiles=put_changes,
        deleteFiles=delete_changes
    )

    return response

def get_token_data(token):
    """
    Fetch data associated with a given token from a predefined API endpoint.
    Args:
    - token (str): The token for which data needs to be fetched.
    Returns:
    - response (Response): The full response from the API. This includes the status code, 
                           headers, and a response containing the data from decoded token.
    """
    
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Make the request to the API
    response = requests.get(API_ENDPOINT, headers=headers)
    
    # Return the whole response
    return response


def lambda_handler(event, context):
    
    key = event['queryStringParameters']['Key']
    token = event['queryStringParameters']['userToken']

    auth_response = get_token_data(token)
    
    # Check if the response is not successful
    if auth_response.status_code != 200:
        return auth_response.text
    response_json = json.loads(auth_response.text)
    user_sub = response_json.get('sub', 'Sub not found')
    
    repo_name = user_sub + event['queryStringParameters']['Repository']
    
    # Download the file from S3 as bytes
    s3_file_byte_array = s3_client.get_object(Bucket=s3_bucket, Key=key)['Body'].read()

    # Dictionary to hold the unzipped content
    unzipped_content = {}

    with io.BytesIO(s3_file_byte_array) as zip_in_memory:
        with zipfile.ZipFile(zip_in_memory) as archive:
            for file_info in archive.infolist():
                with archive.open(file_info.filename) as file:
                    # Add the file content to the unzipped_content dictionary
                    unzipped_content[file_info.filename] = file.read()

    # Push the unzipped content to CodeCommit and return the response
    return {
        'statusCode': 200,
        'body': 'Operation completed with commit ID: ' + str(push_to_codecommit(repo_name, 'main', unzipped_content))
    }
