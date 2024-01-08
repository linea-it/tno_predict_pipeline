#!/bin/bash

echo "Starting TNO Celery Beat" ;
celery -A tno_celery beat \
-l DEBUG \
-s /tmp/celerybeat-schedule \
--pidfile="/tmp/celeryd.pid" \
--logfile="/log/celeryd.log"
