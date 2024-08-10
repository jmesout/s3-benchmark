import boto3
import time
import os
import csv
import matplotlib.pyplot as plt
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from the .env file
load_dotenv()

def create_s3_client():
    """
    Create and return an S3 client configured with credentials and endpoint from environment variables.
    This function uses the boto3 library to create an S3 client with the specified credentials and endpoint.
    """
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    
    # Initialize the S3 client with custom configurations
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version='s3v4')
    )
    return s3

def download_file(s3, bucket_name, object_key, download_path, config):
    """
    Download a file from S3 using multipart download if necessary, and return the time taken.
    The download is timed to measure the performance, which is critical for optimizing the transfer configuration.
    """
    start_time = time.time()  # Start timing the download
    s3.download_file(Bucket=bucket_name, Key=object_key, Filename=download_path, Config=config)
    end_time = time.time()  # End timing the download
    time_taken = end_time - start_time  # Calculate the total time taken
    return time_taken

def calculate_speed(time_taken, file_size):
    """
    Calculate and return the download speed in Mbps.
    This function converts the file size and time taken into a speed metric, which is useful for comparing different configurations.
    """
    speed_bps = file_size * 8 / time_taken  # Convert bytes to bits and divide by time in seconds
    speed_mbps = speed_bps / 1e6  # Convert to Megabits per second
    return speed_mbps

def save_results_to_csv(results, filename):
    """
    Save the results to a CSV file, including TransferConfig parameters.
    This ensures that the results of the download tests are recorded for analysis and reporting.
    """
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'File Size (MB)', 
            'Time Taken (s)', 
            'Download Speed (Mbps)',
            'Multipart Threshold (bytes)',
            'Max Concurrency',
            'Multipart Chunksize (bytes)',
            'Use Threads'
        ])
        writer.writerows(results)

def plot_results(results, output_file):
    """
    Plot the download speeds and save the plot as an image.
    Visualizing the results helps in quickly identifying trends and the impact of different parameters on performance.
    """
    file_sizes = [row[0] for row in results]
    download_speeds = [row[2] for row in results]

    # Create a line plot to visualize download speeds by file size
    plt.figure(figsize=(10, 6))
    plt.plot(file_sizes, download_speeds, marker='o')
    plt.title('Download Speed by File Size')
    plt.xlabel('File Size (MB)')
    plt.ylabel('Download Speed (Mbps)')
    plt.grid(True)
    plt.savefig(output_file)
    plt.show()

def main():
    # Load the S3 bucket name from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # Load file sizes to test from the environment variable or use default values
    file_sizes_str = os.getenv('FILE_SIZES', '100,500,1024,5120,10240,20480,51200,102400')
    file_sizes = list(map(int, file_sizes_str.split(',')))
    
    # Load TransferConfig parameters from environment variables or use defaults
    multipart_threshold = int(os.getenv('MULTIPART_THRESHOLD', 50 * 1024 * 1024))  # Default 50MB
    max_concurrency = int(os.getenv('MAX_CONCURRENCY', 10))  # Default 10 threads
    multipart_chunksize = int(os.getenv('MULTIPART_CHUNKSIZE', 50 * 1024 * 1024))  # Default 50MB
    use_threads = os.getenv('USE_THREADS', 'True').lower() in ['true', '1', 't', 'y', 'yes']
    
    # Generate unique file names based on the current datetime stamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file_name = f'download_results_{timestamp}.csv'
    plot_file_name = f'download_speeds_{timestamp}.png'
    
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
    
    # Loop over each file size to test the download speed
    for size in file_sizes:
        object_key = f'example_{size}mb.txt'
        download_path = f'downloaded_{size}mb.txt'
        
        # Download the file from S3 and measure the time taken
        print(f'Downloading {size}MB file from S3...')
        time_taken = download_file(s3, bucket_name, object_key, download_path, config)
        file_size = os.path.getsize(download_path)  # Get the size of the downloaded file
        speed_mbps = calculate_speed(time_taken, file_size)  # Calculate the download speed
        
        print(f"Downloaded {file_size} bytes in {time_taken:.2f} seconds.")
        print(f"Download speed: {speed_mbps:.2f} Mbps")
        
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
        
        # Clean up the downloaded file after testing to free up space
        os.remove(download_path)
        print(f'Downloaded file of size {size}MB deleted.\n')
    
    # Save the results to a CSV file for later analysis
    save_results_to_csv(results, filename=csv_file_name)
    
    # Plot the results and save the plot as an image
    plot_results(results, output_file=plot_file_name)

if __name__ == "__main__":
    main()
