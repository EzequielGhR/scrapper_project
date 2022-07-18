#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

cd "$parent_path"
cd scrapper
echo "running spider..."
scrapy runspider LaClSpider.py
echo "running transform and load steps"
python3 transform_load.py