#!/usr/bin/env bash
set -euo pipefail

echo "Refreshing databases..."
echo "Removing current instances..."
echo "--------------"

dbfiles=(
  "wcmktprod"
  "wcmktnewkeep"
  "wcmktnorth2"
  "buildcost"
)

preview_deletes() {
    for db in "$@"; do
        if [ -f "${db}.db" ]; then
            echo -f "${db}.db" "${db}.db-shm" "${db}.db-wal" "${db}.info"
            echo "Will delete and refresh: ${db}.db"
        else
            echo "File not found: ${db}.db"
        fi
    done
}

confirm() {
    local response

    while true; do
        read -r -p "Proceed with deletion and refresh? [Y/n] " response || response="n"

        case "$response" in
            ""|[Yy])
                return 0
                ;;
            [Nn])
                echo "Operation cancelled."
                exit 0
                ;;
            *)
                echo "Please enter Y or n."
                ;;
        esac
    done
}


delete_files() {
    for db in "$@"; do
        if [ -f "${db}.db" ]; then
            rm -f "${db}.db" "${db}.db-shm" "${db}.db-wal" "${db}.info"
            echo "Deleted: ${db}.db"
        else
            echo "File not found: ${db}.db"
        fi
    done
}

refresh_db() {
    for db in "$@"; do
        echo "Refreshing: $db"
        turso db export "$db" --with-metadata
    done
}

verify_files() {
    for db in "$@"; do
        if [ -f "${db}.db" ]; then
            echo "Verified: ${db}.db"
        else
            echo "File not found: ${db}.db"
            return 1
        fi
    done
}


preview_deletes  "${dbfiles[@]}"
confirm

delete_files "${dbfiles[@]}"
refresh_db "${dbfiles[@]}"
verify_files "${dbfiles[@]}"

echo "Operation complete."
