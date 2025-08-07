import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import gc

os.chdir('C:\\Users\\baseball\\Desktop\\MLB App\\')

def run_script(script_name, lang='python'):
    """Run a script (either Python or R) and capture errors."""
    try:
        if lang == 'python':
            print(f"Running {script_name}...")
            result = subprocess.run(
                ["python", script_name],
                check=True,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            print(result.stdout)  # Print standard output
            print(result.stderr)  # Print error messages
        elif lang == 'R':
            print(f"Starting to run the R script: {script_name}...")
            rscript_path = r"C:\Program Files\R\R-4.4.2\bin\Rscript.exe"
            result = subprocess.run(
                [rscript_path, script_name],
                check=True,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            print(result.stdout)
            print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_name}: {e}")
        print(f"Standard Output: {e.stdout}")
        print(f"Error Output: {e.stderr}")



if __name__ == "__main__":
    # Record the start time
    start_time = datetime.now()
    print(f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # List of Python scripts to run concurrently
    python_scripts = [
        "gather_data.py",
        "people.py",
        "gather_rosters.py",
        "gather_game_logs_parellel.py"
    ]
    

    # Run the Python scripts concurrently
    with ThreadPoolExecutor() as executor:
        executor.map(run_script, python_scripts, ['python'] * len(python_scripts))

    # Clear memory before running the R script
    del python_scripts  # Delete list to free up memory
    gc.collect()  # Force garbage collection
   
    # After Python scripts complete, run the R script
    r_script = "xstats_for_database.R"
    run_script(r_script, lang='R')
    print("R script completed.")

    # Record the end time
    end_time = datetime.now()
    print(f"Script ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {end_time - start_time}")

