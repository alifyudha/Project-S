import os
import re
import subprocess
import shutil
import json
import logging
import sys
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_WORKERS = 32

REPO_LIST = [
    " `https://github.com/dvahana2424-web/sojogamesdatabase1.git` ",
    " `https://github.com/hammerwebsite12/sojogames2` ",
    " `https://github.com/fylsdy/ManifestHub` ",
    " `https://github.com/kkrmpubg/ManifestHub` ",
    " `https://github.com/mnbvcxz112/A` ",
    " `https://github.com/alifyudha/ManifestAutoUpdate.git` ",
    " `https://github.com/Princeboy520/ManifestHub.git` ",
    " `https://github.com/SteamAutoCracks/ManifestHub.git` ",
    " `https://github.com/tymolu233/ManifestAutoUpdate-fix.git` ",
    " `https://github.com/xu654/Manifest.git` ",
    " `https://github.com/wsxsdyx/ManifestAutoUpdate.git` ",
    " `https://github.com/ikun0014/ManifestHub.git` ",
    " `https://github.com/Auiowu/ManifestAutoUpdate.git` ",
    " `https://github.com/alifyudha/random-fork.git` ",
    " `https://github.com/ManifestHub/ManifestHub.git` ",
    " `https://github.com/hansaes/ManifestAutoUpdate.git` ",
    " `https://github.com/luomojim/ManifestAutoUpdate.git` ",
    " `https://github.com/SPIN0ZAi/SB_manifest_DB.git` ",
]

def clean_url(url):
    return url.replace('`', '').strip()

CLEAN_REPO_LIST = [clean_url(url) for url in REPO_LIST]

OUTPUT_FILE = "decryptionkeys.json"
TEMP_DIR = "temp_repos"

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

def process_repo(repo_url, global_keys):
    repo_name = repo_url.split('/')[-1].replace('.git', '')
    repo_path = os.path.join(TEMP_DIR, repo_name)
    
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)
    
    logging.info(f"Cloning {repo_url}...")
    try:
        # Use partial clone (blobless) to download only history/trees, not file contents
        # This saves bandwidth and space, downloading files only when accessed via git show
        subprocess.run(["git", "clone", "--bare", "--filter=blob:none", repo_url, repo_path], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to clone {repo_url}: {e}")
        return

    # List all branches
    try:
        result = subprocess.run(["git", "for-each-ref", "--format=%(refname:short)", "refs/heads"], 
                                cwd=repo_path, capture_output=True, text=True, check=True)
        branches = result.stdout.splitlines()
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list branches for {repo_name}: {e}")
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
        return

    logging.info(f"Found {len(branches)} branches in {repo_name}. Processing with {MAX_WORKERS} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all branch tasks
        future_to_branch = {executor.submit(process_branch, repo_path, branch): branch for branch in branches}
        
        count = 0
        total = len(branches)
        
        for future in concurrent.futures.as_completed(future_to_branch):
            branch = future_to_branch[future]
            try:
                found_keys = future.result()
                # Merge into global keys (not thread safe if writing directly, but we are in main thread loop here)
                for appid, key in found_keys.items():
                    if appid not in global_keys:
                        global_keys[appid] = key
                    else:
                        if key: # Update if we found a key where previously there might be none
                            global_keys[appid] = key
            except Exception as e:
                logging.warning(f"Error processing branch {branch}: {e}")
            
            count += 1
            if count % 1000 == 0:
                logging.info(f"Processed {count}/{total} branches...")

    # Cleanup
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)
    logging.info(f"Finished {repo_name}")

def main():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
        
    global_keys = {}
    
    for repo in CLEAN_REPO_LIST:
        process_repo(repo, global_keys)
        
    # Remove temp dir
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        
    # Write result
    logging.info(f"Writing {len(global_keys)} keys to {OUTPUT_FILE}")
    
    # Sort keys for consistent output
    sorted_keys = dict(sorted(global_keys.items(), key=lambda item: int(item[0]) if item[0].isdigit() else item[0]))
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(sorted_keys, f, indent=4)

if __name__ == "__main__":
    main()
