#!/bin/bash 

cd `dirname $0`

if [ -z "$DSE_ENV" ]; then
    for include in "~/.dse-env.sh" \
                   "`dirname $0`/dse-env.sh" \
                   "`dirname $0`/../../bin/dse-env.sh" \
                   "/etc/dse/dse-env.sh" ; do
        if [ -r "$include" ]; then
            DSE_ENV="$include"
            break
        fi
    done
fi

if [ -z "$DSE_ENV" ]; then
    echo "DSE_ENV could not be determined."
    exit 1
elif [ -r "$DSE_ENV" ]; then
    . "$DSE_ENV"
else
    echo "Location pointed by DSE_ENV not readable: $DSE_ENV"
    exit 1
fi

CLASSPATH="solr_stress.jar:$CLASSPATH"
java $JVM_OPTS $DSE_OPTS -Dlogback.configurationFile=logback-tools.xml $JAVA_AGENT -ea -Xmx1G -Xms1G -cp $CLASSPATH com.romankagan.dse.demos.solr.SolrStress $*
