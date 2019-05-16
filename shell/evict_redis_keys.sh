#! /bin/bash

#set -x

REDIS='redis-cli'
BACKUP_FILE="backup" # backup deleted keys

function getCursor() {
    awk 'NR == 1'
}

function getKeys() {
    awk 'NR > 1'
}

function delKeys() {
    local ttl_limit=$1
    if [[ -z "$ttl_limit" ]]; then
        ttl_limit=1000000000
    fi

    awk -v redis="$REDIS" -v ttl_limit="$ttl_limit" -v backup_file="$BACKUP_FILE" '
    {
        key = $1
        ttl = $NF
        if(ttl < ttl_limit) {
            print "del " key |& redis
            redis |& getline result
            if(result == 1) {
                print $0 >> backup_file
            }
        }
    }
    END {
        close(redis)
    }
    '
}

function ttlKeys() {
    local ttl_limit=$1
    if [[ -z "$ttl_limit" ]]; then
        ttl_limit=1000000000
    fi

    awk -v redis="$REDIS" -v ttl_limit="$ttl_limit" '
    {
        print ("ttl " $0) |& redis
        redis |& getline ttl
        if(ttl <= ttl_limit) {
            print $0, ttl
        }
    }
    END {
        close(redis)
    }'
}

function cleanOneBatch() {
    local ttl_limit=$1
    getKeys | ttlKeys $ttl_limit | delKeys $ttl_limit
}

function log() {
    echo $(date +'%Y-%m-%dT%H:%M:%S') $@ >& 2
}

function usage() {
    echo "Usage: $0 ttl_limit [scan_limit] [scan_batch]"
}

function main() {
    if [[ $# -lt 1 || $1 == '-h' ]]; then
        usage
        exit 1
    fi
    local ttl_limit=$1
    local limit=$2
    local batch=$3
    if [[ -z "$limit" || "$limit" -lt 0 ]]; then 
        limit=1000000000
    fi
    if [[ -z "$batch" ]]; then
        batch=500
    fi
    local cursor=0
    local count=0

    echo "scan $cursor COUNT $batch" | ${REDIS} > .tmp 
    cursor=$(cat .tmp | getCursor)
    cat .tmp | cleanOneBatch $ttl_limit
    ((count = count + "$batch"))

    while [[ $cursor -gt 0 && $count -lt $limit ]]; do
        echo "scan $cursor COUNT $batch" | ${REDIS} > .tmp
        cursor=$(cat .tmp | getCursor)
        cat .tmp | cleanOneBatch $ttl_limit
        ((count = count + "$batch"))
        log Scanned $count keys
    done
}

main $@
