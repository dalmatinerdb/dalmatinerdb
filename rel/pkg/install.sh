#!/usr/bin/bash

AWK=/usr/bin/awk
SED=/usr/bin/sed

USER=dalmatiner
GROUP=$USER

case $2 in
    PRE-INSTALL)
        if grep "^$GROUP:" /etc/group > /dev/null 2>&1
        then
            echo "Group already exists, skipping creation."
        else
            echo Creating dalmatinerdb group ...
            groupadd $GROUP
        fi
        if id $USER > /dev/null 2>&1
        then
            echo "User already exists, skipping creation."
        else
            echo Creating dalmatinerdb user ...
            useradd -g $GROUP -d /data/dalmatinerdb -s /bin/false $USER
        fi
        echo Creating directories ...
        mkdir -p /data/dalmatinerdb/etc
        mkdir -p /data/dalmatinerdb/log/sasl
        mkdir -p /data/dalmatinerdb/db/ring
        chown -R $USER:$GROUP /data/dalmatinerdb
        if [ -d /tmp/dalmatinerdb ]
        then
            chown -R $USER:$GROUP /tmp/dalmatinerdb
        fi
        ;;
    POST-INSTALL)
        echo Importing service ...
        svccfg import /opt/local/dalmatinerdb/share/ddb.xml
        echo Trying to guess configuration ...
        IP=`ifconfig net0 | grep inet | $AWK '{print $2}'`
        CONFFILE=/data/dalmatinerdb/etc/dalmatinerdb.conf
        cp /opt/local/dalmatinerdb/etc/dalmatinerdb.conf.example ${CONFFILE}.example
        if [ ! -f "${CONFFILE}" ]
        then
            echo "Creating new configuration from example file."
            cp ${CONFFILE}.example ${CONFFILE}
            $SED -i bak -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
        else
            echo "Please make sure you update your config according to the update manual!"
        fi

        OT=/data/dalmatinerdb/etc/rules.ot
        if [ ! -f "${OT}" ]
        then
            echo "none() -> drop." > ${OT}
        fi
        ;;
esac
