import csv
import logging
import random
import time

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Program Constants
INPUT_FILE_NAME = "AIDS_Classification.csv"
OUTPUT_FILE_NAME = "out9.txt"

def prepare_message_from_row(row):
    """Prepare a binary message from a given row."""
    try:
        # Unpack row data into individual variables (adjust based on your CSV structure)
        time, trt, age, wtkg, hemo, homo, drugs, karnof, oprior, z30, preanti, race, gender, str2, strat, symptom, treat, offtrt, cd40, cd420, cd80, cd820, infected = row
        
        # Create a formatted message
        fstring_message = f"[{time}, {trt}, {age}, {wtkg}, {hemo}, {homo}, {drugs}, {karnof}, {oprior}, {z30}, {preanti}, {race}, {gender}, {str2}, {strat}, {symptom}, {treat}, {offtrt}, {cd40}, {cd420}, {cd80}, {cd820}, {infected}]"
        
        # Log the prepared message
        logging.debug(f"Prepared message: {fstring_message}")
        
        # Return the formatted message
        return fstring_message
    
    except Exception as e:
        logging.error(f"Error preparing message from row: {e}")
        return None

def stream_data(input_file_name, output_file_name):
    """Read from input file and write data to output file."""
    logging.info(f"Starting to stream data from {input_file_name} to {output_file_name}.")
    
    try:
        with open(input_file_name, 'r', newline='') as input_file, open(output_file_name, 'w') as output_file:
            logging.info(f"Opened {input_file_name} for reading and {output_file_name} for writing.")
            
            reader = csv.reader(input_file)
            header = next(reader)  # Skip header row
            logging.info(f"Skipped header row: {header}")
            
            # Iterate through each row in the CSV file
            for row in reversed(list(reader)):
                message = prepare_message_from_row(row)  # Prepare message from the row
                if message:
                    output_file.write(message + "\n")  # Write the message to the output file
                    output_file.flush()  # Ensure message is written to file
                    logging.info(f"Written: {message} to {output_file_name}.")
                    time.sleep(random.randint(1, 3))  # Sleep between 1 and 3 seconds 
    
    except FileNotFoundError:
        logging.error(f"Input file '{input_file_name}' not found.")
    
    except Exception as e:
        logging.error(f"An error occurred while streaming data: {e}")

# Execute the stream_data function if the script is run directly
if __name__ == "__main__":
    stream_data(INPUT_FILE_NAME, OUTPUT_FILE_NAME)
