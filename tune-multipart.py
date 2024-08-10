import boto3
import time
import os
import csv
import itertools
import matplotlib.pyplot as plt
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from the .env file
load_dotenv()

def create_s3_client():
    """
    Create and return an S3 client configured with credentials and endpoint from environment variables.
    """
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    
    # Create the S3 client using the provided credentials and endpoint
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
    """
    start_time = time.time()  # Start timing the download
    s3.download_file(Bucket=bucket_name, Key=object_key, Filename=download_path, Config=config)
    end_time = time.time()  # End timing the download
    time_taken = end_time - start_time  # Calculate the total time taken
    return time_taken

def calculate_speed(time_taken, file_size):
    """
    Calculate and return the download speed in Mbps.
    """
    speed_bps = file_size * 8 / time_taken  # Convert bytes to bits and divide by time in seconds
    speed_mbps = speed_bps / 1e6  # Convert to Megabits per second
    return speed_mbps

def save_results_to_csv(results, filename):
    """
    Save the tuning results to a CSV file.
    """
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'Multipart Threshold (bytes)', 
            'Max Concurrency', 
            'Multipart Chunksize (bytes)', 
            'Use Threads', 
            'Time Taken (s)', 
            'Download Speed (Mbps)'
        ])
        writer.writerows(results)

def plot_results(results, output_file):
    """
    Plot the download speeds and save the plot as an image.
    """
    multipart_thresholds = [row[0] for row in results]
    max_concurrencies = [row[1] for row in results]
    multipart_chunksizes = [row[2] for row in results]
    use_threads = [row[3] for row in results]
    speeds = [row[5] for row in results]

    # Generate a scatter plot for each combination of parameters
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        multipart_thresholds, 
        speeds, 
        c=max_concurrencies, 
        cmap='viridis', 
        marker='o', 
        edgecolor='k',
        s=[100 if use == 'True' else 50 for use in use_threads],
        alpha=0.7
    )
    plt.colorbar(scatter, label='Max Concurrency')
    plt.title('Download Speed by Multipart Threshold')
    plt.xlabel('Multipart Threshold (bytes)')
    plt.ylabel('Download Speed (Mbps)')
    plt.grid(True)
    plt.savefig(output_file)
    plt.show()

def find_fastest_parameters(results):
    """
    Find and return the parameters with the highest download speed.
    """
    best_result = max(results, key=lambda x: x[5])  # Find the row with the highest download speed
    return {
        'Multipart Threshold (bytes)': best_result[0],
        'Max Concurrency': best_result[1],
        'Multipart Chunksize (bytes)': best_result[2],
        'Use Threads': best_result[3],
        'Time Taken (s)': best_result[4],
        'Download Speed (Mbps)': best_result[5],
    }

def save_fastest_to_json(fastest_params, filename):
    """
    Save the fastest configuration parameters to a JSON file.
    """
    with open(filename, 'w') as json_file:
        json.dump(fastest_params, json_file, indent=4)

def main():
    # Load necessary configurations from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    tune_file_size = int(os.getenv('TUNE_FILE_SIZE', 1024))  # Default 1GB

    # Load tuning ranges from environment variables
    multipart_thresholds = list(map(int, os.getenv('TUNE_MULTIPART_THRESHOLD').split(',')))
    max_concurrencies = list(map(int, os.getenv('TUNE_MAX_CONCURRENCY').split(',')))
    multipart_chunksizes = list(map(int, os.getenv('TUNE_MULTIPART_CHUNKSIZE').split(',')))
    use_threads_options = os.getenv('TUNE_USE_THREADS').split(',')

    # Prepare combinations of parameters for tuning
    parameter_combinations = itertools.product(
        multipart_thresholds, max_concurrencies, multipart_chunksizes, use_threads_options
    )

    # Create a custom S3 client
    s3 = create_s3_client()
    print('Connected to S3')
    
    object_key = f'example_{tune_file_size}mb.txt'
    download_path = f'downloaded_{tune_file_size}mb.txt'
    
    results = []
    
    # Iterate over each combination of tuning parameters
    for multipart_threshold, max_concurrency, multipart_chunksize, use_threads in parameter_combinations:
        use_threads_bool = use_threads.lower() in ['true', '1', 't', 'y', 'yes']
        
        # Configure the transfer settings for speed optimization
        config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            use_threads=use_threads_bool
        )
        
        # Download the file from S3 and measure the performance
        print(f'Testing configuration: Threshold={multipart_threshold}, Concurrency={max_concurrency}, '
              f'Chunksize={multipart_chunksize}, UseThreads={use_threads}')
        time_taken = download_file(s3, bucket_name, object_key, download_path, config)
        file_size = os.path.getsize(download_path)
        speed_mbps = calculate_speed(time_taken, file_size)
        
        print(f"Downloaded {file_size} bytes in {time_taken:.2f} seconds. Speed: {speed_mbps:.2f} Mbps")
        
        # Store the result for this configuration
        results.append([
            multipart_threshold, 
            max_concurrency, 
            multipart_chunksize, 
            use_threads, 
            time_taken, 
            speed_mbps
        ])
        
        # Clean up the downloaded file after testing
        os.remove(download_path)
        print(f'Downloaded file of size {tune_file_size}MB deleted.\n')
    
    # Generate file names for the CSV and plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file_name = f'tuning_results_{timestamp}.csv'
    plot_file_name = f'tuning_plot_{timestamp}.png'
    json_file_name = f'fastest_configuration_{timestamp}.json'
    
    # Save tuning results to CSV
    save_results_to_csv(results, filename=csv_file_name)
    
    # Plot the results and save the plot
    plot_results(results, output_file=plot_file_name)
    
    # Find and display the best parameters
    best_params = find_fastest_parameters(results)
    print("\nFastest Configuration:")
    for key, value in best_params.items():
        print(f"{key}: {value}")
    
    # Save the best parameters to a JSON file
    save_fastest_to_json(best_params, json_file_name)

if __name__ == "__main__":
    main()
