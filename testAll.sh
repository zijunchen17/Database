#!/bin/bash

rm -rf ECS165a/
echo Begin main
echo Begin main >> log
python3 __main__.py >> log
echo Finished main
echo Finished main >> log
rm -rf ECS165a/

echo Begin m2_tester_part1.py
echo Begin m2_tester_part1.py >> log
python3 m2_tester_part1.py >> log
echo Finished m2_tester_part1.py
echo Finished m2_tester_part1.py >> log

echo Begin m2_tester_part2.py
echo Begin m2_tester_part2.py >> log
python3 m2_tester_part2.py >> log
echo Finished m2_tester_part2.py >> log
echo Finished m2_tester_part2.py

rm -rf ECS165a/
echo Begin index_tester.py
echo Begin index_tester.py >> log
python3 index_tester.py
echo Finished index_tester.py
echo Finished index_tester.py >> log
