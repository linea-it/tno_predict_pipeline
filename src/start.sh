#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

echo "Starting TNO Celery Worker" ;
celery -A tno_celery worker \
--pool solo \
-l DEBUG \
--pidfile="/tmp/%n.pid" \
--logfile="/log/%n%I.log"

