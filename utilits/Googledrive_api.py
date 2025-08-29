from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

# Use persistent credentials so authentication is only required the first time
gauth = GoogleAuth()
gauth.LoadClientConfigFile("utilits/client_secrets.json")
gauth.LoadCredentialsFile("utilits/credentials.json")
if gauth.credentials is None:
    gauth.LocalWebserverAuth()
    gauth.SaveCredentialsFile("utilits/credentials.json")
elif gauth.access_token_expired:
    gauth.Refresh()
    gauth.SaveCredentialsFile("utilits/credentials.json")
else:
    gauth.Authorize()
drive = GoogleDrive(gauth)



def Uplod_latest_file_from_tool(Upload_dir):
    """
        Uploads the latest file from the specified directory to the root of Google Drive.
        Typically used to upload the cleaned file from OUTPUT_FILES.
    """
    # Get the list of files in the upload directory
    # The line `files = [os.path.join(Upload_dir, f) for f in os.listdir(Upload_dir) if
    # os.path.isfile(os.path.join(Upload_dir, f))]` is creating a list of file paths within the specified
    # `Upload_dir` directory. Here's a breakdown of what it does:
    files = [os.path.join(Upload_dir, f) for f in os.listdir(Upload_dir) if os.path.isfile(os.path.join(Upload_dir, f))]
    if not files:
        print("No files found in the upload directory.")
        return None
    # Find the latest file by modification time
    latest_file = max(files, key=os.path.getmtime)
    file_name = os.path.basename(latest_file)
    # Upload to Google Drive root
    file_drive = drive.CreateFile({'title': file_name, 'parents': [{'id': 'root'}]})
    file_drive.SetContentFile(latest_file)
    file_drive.Upload()
    print(f"Uploaded {latest_file} to Google Drive as {file_drive['title']}")
    return file_drive['id']



def download_latest_file_from_drive_root(download_dir):
    """
    Downloads the latest file from the root of Google Drive to a local directory.

    :param download_dir: Local directory to save the downloaded file.
    :return: The file path of the downloaded latest file, or None if no files found.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    # List all files in the root (no parents filter)
    file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
    if not file_list:
        print("No files found in the Google Drive root.")
        return None
    # Sort by createdDate descending to get the latest
    latest_file = sorted(file_list, key=lambda x: x['createdDate'], reverse=True)[0]
    file_path = os.path.join(download_dir, latest_file['title'])
    latest_file.GetContentFile(file_path)
    print(f"Downloaded latest file: {latest_file['title']} to {file_path}")
    return file_path



if __name__ == "__main__":
    download_dir = r"W:\Data_Ingestion_tool\INPUT_FILES"
    uploaded_dir = r"W:\Data_Ingestion_tool\OUTPUT_FILES"

    # The code snippet you provided is calling the function
    # `download_latest_file_from_drive_root(download_dir)` to download the latest file from the root
    # of Google Drive to a local directory specified by `download_dir`.
    
    
    downloaded_file = download_latest_file_from_drive_root(download_dir)
    if downloaded_file:
        print(f"File downloaded to: {downloaded_file}")
    else:
        print("No file was downloaded.")


    
    # The code snippet you provided is calling the function
    # `Uplod_latest_file_from_tool(uploaded_dir)` to upload the latest file from the specified
    # directory to the root of Google Drive.


    Uplod_latest_file_from_tool(uploaded_dir)

    if Uplod_latest_file_from_tool:
        print(f"File has been uploded from the {uploaded_dir}")
    else:
        print("No File was uploded./")




























