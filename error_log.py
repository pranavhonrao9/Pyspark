#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
from slacker import Slacker
import sys
from operator import add
import re

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

slack = Slacker('token')

p1= re.compile(r'(\(.*\))')
url="https://i7p.wdf.sap.corp/sap(bD1lbiZjPTAwMQ==)/bc/bsp/sno/ui_entry/entry.htm?param=69765F6D6F64653D3030312669765F7361706E6F7465735F6E756D6265723D3233333231373026"
error_message = "Composite Memory Limit Violation"


def processRecord(record):
    global slack
    print(record +"\n") 
    slack.chat.post_message('#error_log',record)


def mapper_func(line):
    global error_message
    global url  
    return error_message +"\t"+ p1.findall(line)[0]+"\t"+url

if __name__ == "__main__":
    

    
    conf = SparkConf().setAppName("error_log")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    textFile =sc.textFile("/Users/pranavhonrao/Desktop/composite_oom.txt")
    time_mem_limit = textFile.filter(lambda line : "[MEMORY_LIMIT_VIOLATION]  Information about current memory composite-limit violation:" in line)
    #url="https://i7p.wdf.sap.corp/sap(bD1lbiZjPTAwMQ==)/bc/bsp/sno/ui_entry/entry.htm?param=69765F6D6F64653D3030312669765F7361706E6F7465735F6E756D6265723D3233333231373026"
    #error_messgae = "Composite Memory Limit Violation" 
    error_log = time_mem_limit.map(mapper_func)
    #kt1 =sc.textFile("/Users/pranavhonrao/Desktop/spark-2.0.0-bin-hadoop2.7/url_file.text")
    
    error_log.foreach(processRecord)
   
    #slack = Slacker('xoxp-159516494342-158835554994-159718115895-e8d1cea8e4980a68273c261f704a4210')
    
    #slack.chat.post_message('#general', k )
    

