#!/usr/bin/bash

USER=dalmatiner
GROUP=$USER

case $2 in
    PRE-INSTALL)
        #if grep '^Image: base64 1[34].[1234].*$' /etc/product
	#then
	#    echo "Image version supported"
	#else
	#    echo "This image version is not supported please use the base64 13.2.1 image."
	#    exit 1
        #fi
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
            useradd -g $GROUP -d /var/db/dalmatinerdb -s /bin/false $USER
        fi
        echo Creating directories ...
        mkdir -p /var/db/dalmatinerdb
        chown -R $USER:$GROUP /var/db/dalmatinerdb
        chown -R $USER:$GROUP /var/db/dalmatinerdb/ring
        mkdir -p /var/log/dalmatinerdb/sasl
        chown -R $USER:$GROUP /var/log/dalmatinerdb
        if [ -d /tmp/dalmatinerdb ]
        then
            chown -R $USER:$GROUP /tmp/dalmatinerdb
        fi
        ;;
    POST-INSTALL)
        echo Importing service ...
        svccfg import /opt/local/dalmatinerdb/share/ddb.xml
        echo Trying to guess configuration ...
        IP=`ifconfig net0 | grep inet | awk -e '{print $2}'`
        CONFFILE=/opt/local/dalmatinerdb/etc/dalmatinerdb.conf
        if [ ! -f "${CONFFILE}" ]
        then
            echo "Creating new configuration from example file."
            cp ${CONFFILE}.example ${CONFFILE}
            sed --in-place -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
        else
            echo "Merging old file with new template, the original can be found in ${CONFFILE}.old."
            /opt/local/dalmatinerdb/share/update_config.sh ${CONFFILE}.example ${CONFFILE} > ${CONFFILE}.new &&
                mv ${CONFFILE} ${CONFFILE}.old &&
                mv ${CONFFILE}.new ${CONFFILE}
        fi
        ;;
esac
