import boto3
import time
import os
import csv
import matplotlib.pyplot as plt
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from the .env file to securely manage configurations
load_dotenv()

def create_s3_client():
    """
    Create and return an S3 client configured with credentials and endpoint from environment variables.
    This client is used to interact with the S3 service, including uploading files.
    """
    access_key = os.getenv('AWS_ACCESS_KEY_ID')  # Fetch AWS access key from environment variables
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')  # Fetch AWS secret key from environment variables
    endpoint_url = os.getenv('S3_ENDPOINT_URL')  # Fetch the S3 endpoint URL
    
    # Initialize and return the S3 client with the provided credentials and endpoint
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version='s3v4')
    )
    return s3

def create_dummy_file(file_name, size_in_mb):
    """
    Create a dummy file of the specified size in MB.
    This file will be used to test the upload speed to S3.
    """
    with open(file_name, 'wb') as f:
        f.write(os.urandom(size_in_mb * 1024 * 1024))  # Write random bytes to create a file of the specified size

def upload_file(s3, bucket_name, file_name, object_key, config):
    """
    Upload a file to S3 using multipart upload if necessary, and return the time taken.
    This function is crucial for testing different configurations to optimize upload performance.
    """
    start_time = time.time()  # Start timing the upload
    s3.upload_file(Filename=file_name, Bucket=bucket_name, Key=object_key, Config=config)
    end_time = time.time()  # End timing the upload
    time_taken = end_time - start_time  # Calculate the total time taken for the upload
    return time_taken

def calculate_speed(time_taken, file_size):
    """
    Calculate and return the upload speed in Mbps.
    This metric is essential for evaluating the performance of different upload configurations.
    """
    speed_bps = file_size * 8 / time_taken  # Convert bytes to bits and divide by time in seconds
    speed_mbps = speed_bps / 1e6  # Convert to Megabits per second
    return speed_mbps

def save_results_to_csv(results, filename):
    """
    Save the results to a CSV file, including TransferConfig parameters.
    Storing results allows for analysis and comparison of different configurations over time.
    """
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'File Size (MB)', 
            'Time Taken (s)', 
            'Upload Speed (Mbps)',
            'Multipart Threshold (bytes)',
            'Max Concurrency',
            'Multipart Chunksize (bytes)',
            'Use Threads'
        ])
        writer.writerows(results)

def plot_results(results, output_file):
    """
    Plot the upload speeds and save the plot as an image.
    Visualizing the results helps to quickly identify trends and the effectiveness of different configurations.
    """
    file_sizes = [row[0] for row in results]
    upload_speeds = [row[2] for row in results]

    # Create a plot to visualize upload speed against file size
    plt.figure(figsize=(10, 6))
    plt.plot(file_sizes, upload_speeds, marker='o')
    plt.title('Upload Speed by File Size')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Upload Speed (Mbps)')
    plt.grid(True)
    plt.savefig(output_file)  # Save the plot as an image file
    plt.show()

def main():
    # Load S3 bucket name from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # Load file sizes to test from environment variables or use default values
    file_sizes_str = os.getenv('FILE_SIZES', '100,500,1024,5120,10240,20480,51200,102400')
    file_sizes = list(map(int, file_sizes_str.split(',')))
    
    # Load TransferConfig parameters from environment variables or use default values
    multipart_threshold = int(os.getenv('MULTIPART_THRESHOLD', 50 * 1024 * 1024))  # Default 50MB
    max_concurrency = int(os.getenv('MAX_CONCURRENCY', 10))  # Default 10 threads
    multipart_chunksize = int(os.getenv('MULTIPART_CHUNKSIZE', 50 * 1024 * 1024))  # Default 50MB
    use_threads = os.getenv('USE_THREADS', 'True').lower() in ['true', '1', 't', 'y', 'yes']
    
    # Generate unique file names based on the current datetime stamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file_name = f'upload_results_{timestamp}.csv'
    plot_file_name = f'upload_speeds_{timestamp}.png'
    
    # Create a custom S3 client for interacting with the S3 service
    s3 = create_s3_client()
    print('Connected to S3')
    
    # Configure the transfer settings for speed optimization based on the parameters
    config = TransferConfig(
        multipart_threshold=multipart_threshold,
        max_concurrency=max_concurrency,
        multipart_chunksize=multipart_chunksize,
        use_threads=use_threads
    )
    
    results = []
    
    # Loop over each file size to test the upload speed
    for size in file_sizes:
        file_name = f'dummy_{size}mb.txt'
        object_key = f'example_{size}mb.txt'
        
        # Create a dummy file of the specified size
        print(f'Creating dummy file of size {size}MB...')
        create_dummy_file(file_name, size)
        print(f'Dummy file of size {size}MB created.')
        
        # Upload the file to S3 and measure the time taken
        print(f'Uploading {size}MB file to S3...')
        time_taken = upload_file(s3, bucket_name, file_name, object_key, config)
        file_size = os.path.getsize(file_name)  # Get the size of the dummy file
        speed_mbps = calculate_speed(time_taken, file_size)  # Calculate the upload speed
        
        print(f"Uploaded {file_size} bytes in {time_taken:.2f} seconds.")
        print(f"Upload speed: {speed_mbps:.2f} Mbps")
        
        # Store the result for this file, including TransferConfig parameters
        results.append([
            size, 
            time_taken, 
            speed_mbps,
            multipart_threshold,
            max_concurrency,
            multipart_chunksize,
            use_threads
        ])
        
        # Clean up the local dummy file after the upload to free up space
        os.remove(file_name)
        print(f'Local file of size {size}MB deleted.\n')
    
    # Save the upload results to a CSV file for further analysis
    save_results_to_csv(results, filename=csv_file_name)
    
    # Plot the results and save the plot as an image
    plot_results(results, output_file=plot_file_name)

if __name__ == "__main__":
    main()
