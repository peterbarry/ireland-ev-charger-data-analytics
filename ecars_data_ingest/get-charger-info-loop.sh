#!/bin/bash

for ((; ; ))
do
  now=$(date +%d_%m_%Y_%H:%M:%S)
  target_file_name="./saved_data/$now.charging-locations.kml"
  echo $target_file_name
  curl http://www.esb.ie/electric-cars/kml/charging-locations.kml > $target_file_name
  bzip2 $target_file_name
  sleep 1
done

