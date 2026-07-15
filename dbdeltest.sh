#!/usr/bin/env bash
set -euo pipefail

prod=(
    "wcmktnewkeep"
    "wcmktnorth2"
    "wcmktbkg"
    "buildcost"
)

test=(
    "wcmktnewkeeptest"
    "wcmktnorth2test"
    "wcmktbkgtest"
    "buildcosttest"
)

parse_args() {
    case "${1:-prod}" in
        prod)
            dbfiles=("${prod[@]}")
            echo "Using production databases."
            ;;
        test)
            dbfiles=("${test[@]}")
            echo "Using test databases."
            ;;
        *)
            echo "Usage: $0 [prod|test]" >&2
            exit 2
            ;;
    esac
}

db_exists() {
    local db=$1

    [[ -e "${db}.db" ||
       -e "${db}.db-shm" ||
       -e "${db}.db-wal" ||
       -e "${db}.db-info" ||
       -e "${db}.db-changes" ||
       -e "${db}.db-wal-revert"
    ]]
}

preview_deletes() {
    local db

    for db in "$@"; do
        if db_exists "$db"; then
            echo "Will delete:"
            echo "  ${db}.db"
            echo "  ${db}.db-shm"
            echo "  ${db}.db-wal"
            echo "  ${db}.db-info"
            echo "  ${db}.db-changes"
            echo "  ${db}.db-wal-revert"

        else
            echo "Files not found for: ${db}"
        fi
    done
}

confirm() {
    local response

    while true; do
        read -r -p "Proceed with deletion and refresh? [Y/n] " response ||
            response="n"

        case "$response" in
            "" | [Yy])
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
    local db

    for db in "$@"; do
        if db_exists "$db"; then
            rm -f \
                "${db}.db" \
                "${db}.db-shm" \
                "${db}.db-wal" \
                "${db}.db-info" \
                "${db}.db-changes" \
                "${db}.db-wal-revert"

            echo "Deleted files for: ${db}"
        else
            echo "Files not found for: ${db}"
        fi
    done
}

verify_files() {
    local db
    local failed=0

    for db in "$@"; do
        if db_exists "$db"; then
            echo "Files still exist for: ${db}" >&2
            failed=1
        else
            echo "Verified deleted: ${db}"
        fi
    done

    return "$failed"
}

parse_args "$@"

echo "Refreshing databases..."
echo "Removing current instances..."
echo "--------------"

preview_deletes "${dbfiles[@]}"
confirm
delete_files "${dbfiles[@]}"
verify_files "${dbfiles[@]}"

echo "Operation complete."
