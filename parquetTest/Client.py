import polars as pl
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import io
import requests
import time
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

# Step 1: Create a DataFrame with 5 million rows using Polars
def gen_file():
    num_rows = 50_000_000
    data = {
        'column1': np.random.randint(0, 100, num_rows),
        'column2': np.random.rand(num_rows),
        'column3': np.random.choice(['A', 'B', 'C', 'D'], num_rows)
    }

    df = pl.DataFrame(data)
    return df


# Step 2: Save the DataFrame as a Parquet file in memory
def save_in_memory(df):
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)  # Rewind the buffer to the beginning
    return buffer

#
# Define a function to monitor the progress of the upload
# def create_callback(encoder):
#     encoder_len = encoder.len
#
#     def callback(monitor):
#         progress = monitor.bytes_read / encoder_len * 100
#         print(f"Upload progress: {progress:.2f}%")
#
#     return callback

# Step 3: Send the Parquet file over an API in chunks and time the response
def sending_data(buffer):
    api_url = 'http://127.0.0.1:8000/upload'  # Replace with your actual API endpoint

    # multipart_data = MultipartEncoder(
    #     fields={'file': ('data.parquet', buffer, 'application/octet-stream')}
    # )
    # headers = {'Content-Type': multipart_data.content_type}
    # response = requests.post(api_url, data=multipart_data, headers=headers)

    multipart_encoder = MultipartEncoder(
        fields={'file': ('data.parquet', buffer, 'application/octet-stream')}
    )

    # Use the multipart encoder to stream the data in chunks
    response = requests.post(
        api_url,
        data=multipart_encoder,  # Use data instead of files when using multipart encoder
        headers={'Content-Type': multipart_encoder.content_type}  # Specify the content type
    )

    # files = {'file': ('data.parquet', buffer, 'application/octet-stream')}
    # response = requests.post(api_url, files=files)

    # Check the response from the server
    if response.status_code == 200:
        print('File uploaded successfully')
        print('Response:', response.json())
    else:
        print('Failed to upload file', response.status_code, response.text)




if __name__ == "__main__":
    start_time = time.time()
    df = gen_file()
    end_time = time.time()
    print(f"Time taken to get response: {end_time - start_time:.2f} seconds")

    start_time2 = time.time()
    buffer = save_in_memory(df)
    end_time2 = time.time()
    print(f"Time taken to get response: {end_time2 - start_time2:.2f} seconds")

    start_time3 = time.time()
    sending_data(buffer)
    end_time3 = time.time()
    print(f"Time taken to get response: {end_time3 - start_time3:.2f} seconds")

