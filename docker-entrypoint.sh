#!/bin/sh
exec spark-submit /app/src/utopia/process_event/process_event.py "$@"
