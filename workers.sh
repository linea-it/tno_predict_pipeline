#!/bin/bash

# if any of the commands in your code fails for any reason, the entire script fails
# set -o errexit
# fail exit if one of your pipe command fails
# set -o pipefail
# exits if any of your variables is not set
# set -o nounset

CMD=$1

function do_start() {
    echo "Starting TNO Celery Workers" ;
    # celery -A tno_celery worker \
    #     -c 1 \
    #     --detach \
    #     -l INFO \
    #     --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/%n.pid" \
    #     --logfile="/lustre/t1/tmp/tno/pipelines/tmp/%n%I.log"
    celery -A tno_celery worker \
        -c 1 \
        --detach \
        -l INFO \
        --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/%n.pid" \
        --logfile="/lustre/t1/tmp/tno/pipelines/tmp/%n%I.log"        

    echo "Starting TNO Celery Beat" ;
    celery -A tno_celery beat \
        --detach \
        -l INFO \
        -s /lustre/t1/tmp/tno/pipelines/tmp/celerybeat-schedule \
        --pidfile="/lustre/t1/tmp/tno/pipelines/tmp/celeryd.pid" \
        --logfile="/lustre/t1/tmp/tno/pipelines/tmp/celeryd.log"
}

function do_stop() {
    echo "Stopping Workers"
    ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill -9
    echo "Stopping Beat"
    ps auxww | grep 'celery beat' | awk '{print $2}' | xargs kill -9
    echo "Stopping Beat"
    echo "Removing pid file and logs"
    rm tmp/celery*.log
    rm tmp/*.pid
}

function do_status() {
    echo "Status" ;
    ps aux|grep 'celery worker'
}

case "$CMD" in
    start)
        do_start;;
    stop)
        do_stop ;;
    restart)
        do_stop; sleep 3; do_start ;;
    status)
        do_status ;;
    *)
        echo "Usage: $0 start|stop|restart|status|"
esac
