#!/bin/sh
java -cp classes -Xmx1g ir.Engine -d datasets/guardian -l dd2477.png -p patterns.txt -ni
