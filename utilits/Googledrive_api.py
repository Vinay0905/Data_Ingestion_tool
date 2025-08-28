from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

def authenticate_drive():
	"""
	The function `authenticate_drive` uses GoogleAuth to authenticate and return a GoogleDrive object
	for accessing Google Drive.
	:return: The function `authenticate_drive()` returns an authenticated Google Drive object that can
	be used to interact with Google Drive API.
	"""
	gauth = GoogleAuth()
	gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication
	drive = GoogleDrive(gauth)
	return drive

def download_latest_file_from_drive(folder_id, download_dir="INPUT_FILES"):
	"""
	The function `download_latest_file_from_drive` downloads the latest file from a specified Google
	Drive folder to a local directory.
	
	:param folder_id: The `folder_id` parameter in the `download_latest_file_from_drive` function is the
	unique identifier of the Google Drive folder from which you want to download the latest file. This
	ID can be found in the URL of the folder when you are in Google Drive. It typically looks like a
	long string
	:param download_dir: The `download_dir` parameter in the `download_latest_file_from_drive` function
	is a string that specifies the directory where the downloaded file will be saved. By default, if no
	value is provided for `download_dir`, the file will be saved in a directory named "INPUT_FILES".
	However, you, defaults to INPUT_FILES (optional)
	:return: The function `download_latest_file_from_drive` returns the file path of the downloaded
	latest file.
	"""
	drive = authenticate_drive()
	# List all files in the folder
	file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
	if not file_list:
		print("No files found in the Google Drive folder.")
		return None
	# Sort by createdDate descending to get the latest
	latest_file = sorted(file_list, key=lambda x: x['createdDate'], reverse=True)[0]
	file_path = os.path.join(download_dir, latest_file['title'])
	latest_file.GetContentFile(file_path)
	print(f"Downloaded latest file: {latest_file['title']} to {file_path}")
	return file_path

def upload_file_to_drive(local_path, folder_id, new_name=None):
	"""
	The function `upload_file_to_drive` uploads a file from a local path to Google Drive within a
	specified folder, with an optional new name.
	
	:param local_path: The `local_path` parameter in the `upload_file_to_drive` function refers to the
	path of the file on your local machine that you want to upload to Google Drive. This is the file
	that will be transferred to Google Drive during the execution of the function
	:param folder_id: The `folder_id` parameter in the `upload_file_to_drive` function is the unique
	identifier of the folder in Google Drive where you want to upload the file. This ID specifies the
	destination folder where the file will be stored
	:param new_name: The `new_name` parameter in the `upload_file_to_drive` function is an optional
	parameter that allows you to specify a new name for the file when uploading it to Google Drive. If
	you provide a value for `new_name`, the file will be uploaded with that name. If you do not
	:return: The function `upload_file_to_drive` returns the ID of the file that was uploaded to Google
	Drive.
	"""
	drive = authenticate_drive()
	file_drive = drive.CreateFile({'parents': [{'id': folder_id}]})
	file_drive['title'] = new_name if new_name else os.path.basename(local_path)
	file_drive.SetContentFile(local_path)
	file_drive.Upload()
	print(f"Uploaded {local_path} to Google Drive as {file_drive['title']}")
	return file_drive['id']
