#!/bin/bash
COUNT=$1

while [ $COUNT -gt 0 ]; do
	go test 2> /dev/null > out
	grep 'PASS' out
	if [ $? == 0 ]; then
		let COUNT=COUNT-1
	else exit
	fi
done
echo All passed

