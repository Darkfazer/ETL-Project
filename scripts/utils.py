# scripts/utils.py
import os
import logging

def get_project_root():
    """Returns absolute path to project root"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(current_dir)  # Goes up one level from scripts/

def get_data_path(*subpaths):
    """Get absolute path to data file with ensured parent directory"""
    data_dir = os.path.join(get_project_root(), 'data')
    full_path = os.path.join(data_dir, *subpaths)

    # Ensure the parent directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    return full_path

def get_db_path():
    """Get path to SQLite database with ensured directory"""
    db_dir = os.path.join(get_project_root(), 'database')
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "sustainability.db")

def setup_logging(log_file=None):
    """Configure logging to console and optionally to file"""
    handlers = [logging.StreamHandler()]

    if log_file:
        log_path = get_data_path(log_file)
        handlers.append(logging.FileHandler(log_path))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers
    )

# Optional test print to verify imports
if __name__ == "__main__":
    print("utils.py loaded successfully")
    setup_logging()
    logging.info("Logging initialized from utils.")
