mysql -uroot -p123456 << EOF
 use autoHome;
 set names utf8;
source /opt/part_0.sql
EOF
echo "part_0_import end" &
mysql -uroot -p123456 << EOF
 use autoHome;
 set names utf8;
source /opt/part_1.sql
EOF
echo "part_1_import end" &
mysql -uroot -p123456 << EOF
 use autoHome;
 set names utf8;
source /opt/part_2.sql
EOF
echo "part_2_import end" &
mysql -uroot -p123456 << EOF
 use autoHome;
 set names utf8;
source /opt/part_3.sql
EOF
echo "part_3_import end" &