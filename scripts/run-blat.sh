#!/bin/bash

cd /data/dd-analysis/master_blat/
gfServer -tileSize=8 -minMatch=1 start 0.0.0.0 8080 master.2bit

