import os
import re
import subprocess
import shutil
import json
import logging
import sys
import concurrent.futures
import argparse
import time
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REPO_LIST = [
    " `https://github.com/dvahana2424-web/sojogamesdatabase1.git` ",
    " `https://github.com/hammerwebsite12/sojogames2` ",
    " `https://github.com/fylsdy/ManifestHub` ",
    " `https://github.com/kkrmpubg/ManifestHub` ",
    " `https://github.com/mnbvcxz112/A` ",
    " `https://github.com/Princeboy520/ManifestHub.git` ",
    " `https://github.com/SteamAutoCracks/ManifestHub.git` ",
    " `https://github.com/tymolu233/ManifestAutoUpdate-fix.git` ",
    " `https://github.com/xu654/Manifest.git` ",
    " `https://github.com/wsxsdyx/ManifestAutoUpdate.git` ",
    " `https://github.com/ikun0014/ManifestHub.git` ",
    " `https://github.com/Auiowu/ManifestAutoUpdate.git` ",
    " `https://github.com/ManifestHub/ManifestHub.git` ",
    " `https://github.com/hansaes/ManifestAutoUpdate.git` ",
    " `https://github.com/luomojim/ManifestAutoUpdate.git` ",
    " `https://github.com/SPIN0ZAi/SB_manifest_DB.git` ",
]

def clean_url(url):
    return url.replace('`', '').strip()

CLEAN_REPO_LIST = [clean_url(url) for url in REPO_LIST]

OUTPUT_FILE = "decryptionkeys.json"
STATE_FILE = "scan_state.json"
TEMP_DIR = "temp_repos"

# Global lock for file operations and key updates
file_lock = threading.Lock()

def get_keys_from_content(content):
    keys = {}
    # Pattern 1: With key
    # addappid(2840771, 1, "64292a119e4b390ef4488dd942329a7794234989b74c79e3228adb22bfd9d4e9")
    pattern_with_key = re.compile(r'addappid\(\s*(\d+)\s*,\s*\d+\s*,\s*"([^"]+)"\)')
    
    # Pattern 2: Without key
    # addappid(2840770)
    pattern_no_key = re.compile(r'addappid\(\s*(\d+)\s*\)')

    for line in content.splitlines():
        # Check for key first
        match_key = pattern_with_key.search(line)
        if match_key:
            appid = match_key.group(1)
            key = match_key.group(2)
            keys[appid] = key
            continue
        
        # Check for no key
        match_no_key = pattern_no_key.search(line)
        if match_no_key:
            appid = match_no_key.group(1)
            # Only add if not already present (prioritize key over no key)
            if appid not in keys:
                keys[appid] = ""
            continue
            
    return keys

def save_state(current_run_time):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_scan_time": current_run_time}, f)
        logging.info("Saved scan state.")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")

def save_keys_to_file(keys, current_run_time=None):
    """
    Save keys to file atomically.
    """
    with file_lock:
        try:
            # Sort keys for consistent output
            sorted_keys = dict(sorted(keys.items(), key=lambda item: int(item[0]) if item[0].isdigit() else item[0]))
            
            temp_file = OUTPUT_FILE + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(sorted_keys, f, indent=4)
                
            shutil.move(temp_file, OUTPUT_FILE)
            
            # Save state if time provided
            if current_run_time:
                save_state(current_run_time)
            
            # Git commit and push
            try:
                files_to_commit = []
                subprocess.run(["git", "add", OUTPUT_FILE], check=True, capture_output=True)
                files_to_commit.append(OUTPUT_FILE)
                
                # Always try to add state file if it exists
                if os.path.exists(STATE_FILE):
                    subprocess.run(["git", "add", STATE_FILE], check=True, capture_output=True)
                    files_to_commit.append(STATE_FILE)
    
                # Check if there are changes
                status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
                
                has_changes = any(f in status.stdout for f in files_to_commit)
                
                if has_changes:
                    subprocess.run(["git", "commit", "-m", f"Auto-update decryption keys ({len(keys)} keys) [skip ci]"], check=True, capture_output=True)
                    subprocess.run(["git", "push"], check=True, capture_output=True)
                    logging.info(f"Pushed updates to GitHub. Total keys: {len(keys)}")
            except Exception as e:
                logging.warning(f"Git push failed: {e}")
    
        except Exception as e:
            logging.error(f"Failed to save keys: {e}")

def process_branch(repo_path, branch):
    """
    Process a single branch: find lua files and extract keys.
    Returns a dictionary of found keys.
    """
    branch_keys = {}
    try:
        # List files in the branch
        ls_tree = subprocess.run(["git", "ls-tree", "-r", "--name-only", branch], 
                                    cwd=repo_path, capture_output=True, text=True)
        files = ls_tree.stdout.splitlines()
        lua_files = [f for f in files if f.endswith('.lua')]
        
        for lua_file in lua_files:
            # Read file content
            show_cmd = subprocess.run(["git", "show", f"{branch}:{lua_file}"], 
                                        cwd=repo_path, capture_output=True, text=True, errors='ignore')
            content = show_cmd.stdout
            found_keys = get_keys_from_content(content)
            
            # Merge into branch keys
            for appid, key in found_keys.items():
                if appid not in branch_keys:
                    branch_keys[appid] = key
                else:
                    if key:
                        branch_keys[appid] = key
    except Exception as e:
        # Log minimally to avoid spamming if many fail
        pass
        
    return branch_keys

def force_remove_dir(dir_path, retries=5, delay=1):
    """
    Robustly remove a directory, retrying on failure.
    """
    if not os.path.exists(dir_path):
        return

    for i in range(retries):
        try:
            shutil.rmtree(dir_path)
            return
        except OSError as e:
            if i < retries - 1:
                logging.warning(f"Failed to remove {dir_path} (attempt {i+1}/{retries}): {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to remove {dir_path} after {retries} attempts: {e}")
                # Try to ignore errors as a last resort
                shutil.rmtree(dir_path, ignore_errors=True)

def process_repo(repo_url, global_keys, max_workers, last_scan_time=0):
    repo_name = repo_url.split('/')[-1].replace('.git', '')
    repo_path = os.path.join(TEMP_DIR, repo_name)
    
    # Ensure clean slate
    force_remove_dir(repo_path)
    
    logging.info(f"Cloning {repo_url}...")
    try:
        # Use partial clone (blobless) to download only history/trees, not file contents
        # This saves bandwidth and space, downloading files only when accessed via git show
        subprocess.run(["git", "clone", "--bare", "--filter=blob:none", repo_url, repo_path], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to clone {repo_url}: {e}")
        return

    # List all branches with committer date
    # Format: %(refname:short)|%(committerdate:unix)
    try:
        result = subprocess.run(["git", "for-each-ref", "--format=%(refname:short)|%(committerdate:unix)", "refs/heads"], 
                                cwd=repo_path, capture_output=True, text=True, check=True)
        raw_branches = result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list branches for {repo_name}: {e}")
        force_remove_dir(repo_path)
        return

    # Filter branches based on last scan time
    branches_to_process = []
    skipped_count = 0
    
    for line in raw_branches:
        try:
            parts = line.split('|')
            if len(parts) == 2:
                branch_name = parts[0]
                commit_time = int(parts[1])
                
                if commit_time > last_scan_time:
                    branches_to_process.append(branch_name)
                else:
                    skipped_count += 1
        except ValueError:
            continue

    logging.info(f"Found {len(branches_to_process)} modified branches (skipped {skipped_count} old branches) in {repo_name}. Processing with {max_workers} threads...")
    
    if not branches_to_process:
        force_remove_dir(repo_path)
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all branch tasks
        future_to_branch = {executor.submit(process_branch, repo_path, branch): branch for branch in branches_to_process}
        
        count = 0
        total = len(branches_to_process)
        
        for future in concurrent.futures.as_completed(future_to_branch):
            branch = future_to_branch[future]
            try:
                found_keys = future.result()
                # Merge into global keys (not thread safe if writing directly, but we are in main thread loop here)
                # However, with repo concurrency, we need to lock
                with file_lock:
                    for appid, key in found_keys.items():
                        if appid not in global_keys:
                            global_keys[appid] = key
                        else:
                            if key: # Update if we found a key where previously there might be none
                                global_keys[appid] = key
            except Exception as e:
                logging.warning(f"[{repo_name}] Error processing branch {branch}: {e}")
            
            count += 1
            if count % 1000 == 0:
                logging.info(f"[{repo_name}] Processed {count}/{total} branches...")
                # We don't save state here, only keys, to avoid partial state updates
                save_keys_to_file(global_keys)

    # Cleanup
    force_remove_dir(repo_path)
    logging.info(f"Finished {repo_name}")

def main():
    parser = argparse.ArgumentParser(description="Extract decryption keys from Steam manifest repositories.")
    parser.add_argument("--workers", type=int, default=32, help="Number of threads for concurrent branch processing per repo")
    parser.add_argument("--repo-workers", type=int, default=4, help="Number of concurrent repositories to process")
    args = parser.parse_args()
    
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
        
    global_keys = {}
    last_scan_time = 0

    # Load state if exists
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                last_scan_time = state.get("last_scan_time", 0)
            logging.info(f"Loaded last scan time: {last_scan_time}")
        except Exception as e:
            logging.warning(f"Failed to load state: {e}")
    else:
        # Create initial state file if it doesn't exist
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({"last_scan_time": 0}, f)
            logging.info("Created initial scan_state.json")
        except Exception as e:
            logging.warning(f"Failed to create initial state file: {e}")
    
    # Load existing keys if file exists
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r') as f:
                global_keys = json.load(f)
            logging.info(f"Loaded {len(global_keys)} existing keys from {OUTPUT_FILE}")
        except Exception as e:
            logging.warning(f"Failed to load existing keys: {e}")

    # Start time for this run
    current_run_time = int(time.time())

    # Process repos in parallel
    # Default to 4 parallel repos if not specified (hardcoded here or use another arg)
    REPO_WORKERS = args.repo_workers
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=REPO_WORKERS) as repo_executor:
        future_to_repo = {repo_executor.submit(process_repo, repo, global_keys, args.workers, last_scan_time): repo for repo in CLEAN_REPO_LIST}
        
        for future in concurrent.futures.as_completed(future_to_repo):
            repo = future_to_repo[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Repo {repo} failed: {e}")

    # Remove temp dir
    force_remove_dir(TEMP_DIR)
    
    # Final save with state update
    logging.info(f"Writing {len(global_keys)} keys to {OUTPUT_FILE}")
    save_keys_to_file(global_keys, current_run_time)

if __name__ == "__main__":
    main()
