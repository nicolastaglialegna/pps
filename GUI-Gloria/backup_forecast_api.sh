#!/bin/bash

DIA=`date +"%d-%m-%Y"`

PGPASSWORD="tad" pg_dump -h localhost -p 5432 -U tad -t forecast_api -F p -b -v -f "/home/futit/Documentos/MachineLearning/buckup_forecast/forecast_bkp"$DIA".sql" machine
