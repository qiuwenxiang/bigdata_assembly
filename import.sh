mysql -uroot -p123456 << EOF
 use autoHome;
 set names utf8;
source /opt/part_0.sql
source /opt/part_1.sql
source /opt/part_2.sql
source /opt/part_3.sql
EOF
echo "import end"