import json
import os
from datetime import datetime
from pathlib import Path
from sqlite3 import connect
import time
import libsql
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler("sync_debug.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Get project root directory (where the .env file is)
def get_project_root() -> Path:
    """Returns the project root directory."""
    return Path(__file__).parent.parent.parent


def test_libsql():
    project_root = get_project_root()
    db_path = project_root / "wcmkt2.db"
    with libsql.connect(str(db_path)) as conn:
        results = conn.execute("SELECT * FROM marketorders").fetchall()
        print(results[:5])

def get_secrets():
    url = os.getenv("TURSO_WCMKT2_URL")
    token = os.getenv("TURSO_WCMKT2_TOKEN")
    secrets_dict = {
        "url": url,
        "token": token
    }
    return secrets_dict


def test_secrets():
    secrets_dict = get_secrets()
    print(secrets_dict)
    quit()


def test_sync():
    logger.info("Starting test_sync()")
    project_root = get_project_root()
    db_info_path = project_root / "wcmkt2.db-info"
    db_path = project_root / "wcmkt2.db"
    state_pre_path = project_root / "wcmkt_state_pre.txt"
    state_results_path = project_root / "wcmkt_state_results.txt"

    with open(db_info_path, "r") as f:
        pre_info = f.read()
    pre_info = json.loads(pre_info)
    print(f"pre_info: {pre_info}")
    secrets_dict = get_secrets()
    logger.info(f"secrets_dict: {secrets_dict}")
    url = secrets_dict["url"]
    token = secrets_dict["token"]
    logger.info(f"url: {url}")
    logger.info(f"token: {token}")
    with open(state_pre_path, "w") as f:
        f.write(json.dumps(pre_info, indent=4))
    logger.info(f"pre_info saved to wcmkt_state_pre.txt")



    current_time = datetime.now()
    logger.info(f"current_time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    p0 = time.perf_counter()
    with libsql.connect(str(db_path), sync_url=url, auth_token=token) as conn:
       logger.info(f"libsql connection established: starting sync")
       conn.sync()
    p1 = time.perf_counter()
    elapsed_time = round((p1 - p0)*1000, 2)
    logger.info(f"elapsed_time: {elapsed_time} ms")

    with open(db_info_path, "r") as f:
        post_info = f.read()
    post_info = json.loads(post_info)

    results_dict = {
        "pre_info": pre_info,
        "post_info": post_info,
        "start_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "elapsed_time": elapsed_time
    }
    logger.info(f"results_dict: {json.dumps(results_dict, indent=4)}")
    with open(state_results_path, "w") as f:
        f.write(json.dumps(results_dict, indent=4))
    logger.info("completed test_sync()")

def parse_libsql_log(log_path: str):
    with open(log_path, "r") as f:
        lines = f.readlines()
    for line in lines[:5]:
        print(line.strip())
        print("-"*100)

def record_pre_sync():
    logger.info("Starting record_pre_sync()")
    project_root = get_project_root()
    db_info_path = project_root / "wcmkt2.db-info"
    state_pre_path = project_root / "wcmkt_state_pre.txt"

    with open(db_info_path, "r") as f:
        pre_info = f.read()

    pre_info = json.loads(pre_info)
    with open(state_pre_path, "w") as f:
        f.write(json.dumps(pre_info, indent=4))
    logger.info(f"pre_info saved to wcmkt_state_pre.txt")

def record_post_sync():
    logger.info("Starting record_post_sync()")
    project_root = get_project_root()
    db_info_path = project_root / "wcmkt2.db-info"
    state_post_path = project_root / "wcmkt_state_post.txt"

    with open(db_info_path, "r") as f:
        post_info = f.read()
    post_info = json.loads(post_info)
    with open(state_post_path, "w") as f:
        f.write(json.dumps(post_info, indent=4))
    logger.info(f"post_info saved to wcmkt_state_post.txt")
    logger.info("completed record_post_sync()")

def record_results(start_time: datetime, end_time: datetime, function_name: str):
    logger.info("Starting record_results()")
    project_root = get_project_root()
    state_pre_path = project_root / "wcmkt_state_pre.txt"
    state_post_path = project_root / "wcmkt_state_post.txt"
    state_results_path = project_root / ".wcmkt_state_results.json"

    with open(state_pre_path, "r") as f:
        pre_info = f.read()
    pre_info = json.loads(pre_info)
    with open(state_post_path, "r") as f:
        post_info = f.read()
    post_info = json.loads(post_info)
    elapsed_time = round((end_time - start_time)*1000, 2)
    with open(state_results_path, "r") as f:
        results_info = f.read()
    existing_results = json.loads(results_info)

    current_time = datetime.now()

    frames_change = post_info["durable_frame_num"] - pre_info["durable_frame_num"]
    generation_change = post_info["generation"] - pre_info["generation"]
    new_results_dict = {
        "current_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
        "pre_info": pre_info,
        "post_info": post_info,
        "start_time": start_time,
        "end_time": end_time,
        "elapsed_time": elapsed_time,
        "frames_change": frames_change,
        "generation_change": generation_change,
        "function_name": function_name
    }
    existing_results.append(new_results_dict)
    with open(state_results_path, "w") as f:
        f.write(json.dumps(existing_results, indent=4))
    logger.info(f"{function_name} results saved to .wcmkt_state_results.json: {json.dumps(new_results_dict, indent=4)}")
    logger.info("completed record_results()")

if __name__ == "__main__":
    pass
