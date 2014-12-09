#!/usr/bin/env bash

example=$1
old=$2
get() {
    var=$2
    conf=$1

    res=$(grep "^\\s*${var}\\s*=" $conf | sed "s/^\\s*${var}\\s*=\\s*//")
    if [ -z "${res}" ]
    then
        exit 1
    fi
    echo "${res}"
}


while read line
do
    if echo "${line}" | grep -v '^#' | grep -v '^$' > /dev/null
    then
        key=$(echo "${line}" | sed 's/\s*=.*//')
        if val=$(get ${old} ${key})
        then
            echo "${key} = ${val}"

        else
            echo "${line}"
        fi
    else
        echo "${line}"

    fi
done < "${example}"
