# update_script.py
import os
import shutil
import time
import subprocess
import sys
import json
import re

def _parse_jsonc(jsonc_string: str) -> dict:
    """Parse a JSONC string while removing comments."""
    lines = jsonc_string.splitlines()
    no_comments_lines = []
    in_block_comment = False
    for line in lines:
        stripped_line = line.strip()
        if in_block_comment:
            if '*/' in stripped_line:
                in_block_comment = False
                line = stripped_line.split('*/', 1)[1]
            else:
                continue
        
        if '/*' in line and not in_block_comment:
            before_comment, _, after_comment = line.partition('/*')
            if '*/' in after_comment:
                _, _, after_block = after_comment.partition('*/')
                line = before_comment + after_block
            else:
                line = before_comment
                in_block_comment = True

        if line.strip().startswith('//'):
            continue
        
        no_comments_lines.append(line)

    return json.loads("\n".join(no_comments_lines))

def load_jsonc_values(path):
    """Load a .jsonc file and return its values while ignoring comments."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        return _parse_jsonc(content)
    except (FileNotFoundError, json.JSONDecodeError, Exception) as e:
        print(f"Failed to load or parse values from {path}: {e}")
        return None

def get_all_relative_paths(directory):
    """Return relative paths for all files and empty directories."""
    paths = set()
    for root, dirs, files in os.walk(directory):
        # Record files
        for name in files:
            path = os.path.join(root, name)
            paths.add(os.path.relpath(path, directory))
        # Record empty directories
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):
                paths.add(os.path.relpath(dir_path, directory) + os.sep)
    return paths

def main():
    print("--- Update script started ---")
    
    # 1. Wait for the main program to exit
    print("Waiting for the main program to stop (3 seconds)...")
    time.sleep(3)
    
    # 2. Define paths
    destination_dir = os.getcwd()
    update_dir = "update_temp"
    source_dir_inner = os.path.join(update_dir, "LMArenaBridge-main")
    config_filename = 'config.jsonc'
    models_filename = 'models.json'
    model_endpoint_map_filename = 'model_endpoint_map.json'
    
    if not os.path.exists(source_dir_inner):
        print(f"Error: source directory {source_dir_inner} not found. Update aborted.")
        return
        
    print(f"Source directory: {os.path.abspath(source_dir_inner)}")
    print(f"Destination directory: {os.path.abspath(destination_dir)}")

    # 3. Back up critical files
    print("Backing up current configuration and model files...")
    old_config_path = os.path.join(destination_dir, config_filename)
    old_models_path = os.path.join(destination_dir, models_filename)
    old_config_values = load_jsonc_values(old_config_path)
    
    # 4. Determine which files and folders to preserve
    # Keep update_temp itself, the .git directory, and any user-created hidden files/folders
    preserved_items = {update_dir, ".git", ".github"}

    # 5. Collect file listings for comparison
    new_files = get_all_relative_paths(source_dir_inner)
    # Exclude .git and .github from deployment
    new_files = {f for f in new_files if not (f.startswith('.git') or f.startswith('.github'))}

    current_files = get_all_relative_paths(destination_dir)

    print("\n--- File change analysis ---")
    print("[*] Deletions are disabled to protect user data; only copies and config updates will run.")

    # 7. Copy new files (excluding configuration files)
    print("\n[+] Copying new files...")
    try:
        new_config_template_path = os.path.join(source_dir_inner, config_filename)
        
        for item in os.listdir(source_dir_inner):
            s = os.path.join(source_dir_inner, item)
            d = os.path.join(destination_dir, item)
            
            # Skip .git and .github directories
            if item in {".git", ".github"}:
                continue
            
            if os.path.basename(s) == config_filename:
                continue  # Skip the primary config file (merge later)
            
            if os.path.basename(s) == model_endpoint_map_filename:
                continue  # Preserve the local model endpoint mapping

            if os.path.basename(s) == models_filename:
                continue  # Preserve the local models.json

            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
        print("File copy completed successfully.")

    except Exception as e:
        print(f"Error copying files: {e}")
        return

    # 8. Smart-merge the configuration file
    if old_config_values and os.path.exists(new_config_template_path):
        print("\n[*] Performing smart config merge (preserving comments)...")
        try:
            with open(new_config_template_path, 'r', encoding='utf-8') as f:
                new_config_content = f.read()

            new_version_values = load_jsonc_values(new_config_template_path)
            new_version = new_version_values.get("version", "unknown")
            old_config_values["version"] = new_version

            for key, value in old_config_values.items():
                if isinstance(value, str):
                    replacement_value = f'"{value}"'
                elif isinstance(value, bool):
                    replacement_value = str(value).lower()
                else:
                    replacement_value = str(value)
                
                pattern = re.compile(f'("{key}"\s*:\s*)(?:".*?"|true|false|[\d\.]+)')
                if pattern.search(new_config_content):
                    new_config_content = pattern.sub(f'\\g<1>{replacement_value}', new_config_content)

            with open(old_config_path, 'w', encoding='utf-8') as f:
                f.write(new_config_content)
            print("Configuration merged successfully.")

        except Exception as e:
            print(f"Severe error during configuration merge: {e}")
    else:
        print("Smart merge unavailable; copying the new configuration file instead.")
        if os.path.exists(new_config_template_path):
            shutil.copy2(new_config_template_path, old_config_path)

    # 9. Clean up the temporary directory
    print("\n[*] Removing temporary files...")
    try:
        shutil.rmtree(update_dir)
        print("Cleanup complete.")
    except Exception as e:
        print(f"Error removing temporary files: {e}")

    # 10. Restart the main program
    print("\n[*] Restarting the main program...")
    try:
        main_script_path = os.path.join(destination_dir, "api_server.py")
        if not os.path.exists(main_script_path):
             print(f"Error: main script not found at {main_script_path}.")
             return
        
        subprocess.Popen([sys.executable, main_script_path])
        print("Main program relaunched in the background.")
    except Exception as e:
        print(f"Failed to restart the main program: {e}")
        print(f"Please run {main_script_path} manually.")

    print("--- Update complete ---")

if __name__ == "__main__":
    main()
