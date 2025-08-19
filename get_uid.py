import os
import subprocess
from config import DATA_DIR, NCBI_API_KEY, BEGIN_YEAR, END_YEAR
from collections import OrderedDict

def get_uid(min_year, max_year):
    """
    Get the UID for a given year
    """
    output_file = os.path.join(DATA_DIR, f"uid_{min_year}_{max_year}.txt")
    if os.path.exists(output_file):
        print(f"UID file for {min_year}_{max_year} already exists")
        return

    # check if the api key is used
    if NCBI_API_KEY and not os.environ.get("NCBI_API_KEY"):
        os.environ["NCBI_API_KEY"] = NCBI_API_KEY

    query = "hasstructuredabstract"
    cmd = f'esearch -db pubmed -query "{query}" -datetype pdat -mindate {min_year} -maxdate {max_year} | efetch -format uid'

    print(f"start to get uid from {min_year} to {max_year}")
    print('_'*100)

    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    
    # Use OrderedDict to avoid duplicates
    seen = OrderedDict()
    try:
        for line in proc.stdout:
            uid = line.strip()
            if uid:
                seen.setdefault(uid, None)
        retcode = proc.wait()
        if retcode != 0:
            # read and raise error
            err = proc.stderr.read()
            raise RuntimeError(f"EDirect pipeline failed (exit {retcode}): {err.strip()}")
    finally:
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()

    # Atomic write to avoid half-written files
    print("We have got all the uids")
    print('_'*100)
    print(f"start to write to {output_file}")
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    tmp_path = output_file + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for uid in seen.keys():
            f.write(uid + "\n")
    os.replace(tmp_path, output_file)

    print(f"Saved {len(seen)} UIDs for {min_year}_{max_year} to {output_file}")

if __name__ == "__main__":
    get_uid(BEGIN_YEAR, END_YEAR)