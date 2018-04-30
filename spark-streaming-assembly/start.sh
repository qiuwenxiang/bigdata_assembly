jarlib="lib"
CLASS_PATH="start.sh"
for i in `ls $jarlib/*.jar`; do
CLASS_PATH="$CLASS_PATH","$i";
done

APP_MAINCLASS="com.kylin.assembly.spark.sql.WordCountInSpark2"
JAVA_CMD="spark-submit --master yarn  --class $APP_MAINCLASS  --jars $CLASS_PATH --executor-memory 1G --total-executor-cores 7 --deploy-mode cluster --conf 'spark.yarn.appMasterEnv.JAVA_HOME=/opt/leap-jdk8'  --conf 'spark.executorEnv.JAVA_HOME=/opt/leap-jdk8' /opt/spark-streaming/spark-streaming-assembly.jar"
echo $JAVA_CMD
eval $JAVA_CMD