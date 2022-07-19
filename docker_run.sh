#!/bin/bash

docker build -t "etl_pipe" .
docker run -it -p 8888:8888 etl_pipe /bin/bash