#!/bin/bash
find /var/www/html/synthesized/ -mmin +120 -exec rm -f {} \;
python3 /var/wsbdd/praw/clear_db.py \;