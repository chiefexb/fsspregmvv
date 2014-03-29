#!/bin/sh
USER='update_ppo'
PASSWD='123'
cd /home/chief/rr_out
#cd /home/path/to/public_html/tmp/orders/
for filename in $(ls *.xml); do
 
 ftp -n 10.9.4.11 <<EOF
 quote USER $USER
 quote PASS $PASSWD
 passive
 cd /report/rr_out
 put $filename $filename
 close
 bye
EOF
mv $filename /home/chief/rr_out_arc
done;
