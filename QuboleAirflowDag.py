from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators import QuboleOperator
from airflow.operators import DummyOperator, SubDagOperator
from airflow.example_dags.subdags.subdag import subdag


default_args = {
    'owner': 'ajithr-test',
    'start_date': datetime.today(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('ABTest_Spark_minutes_3', schedule_interval=timedelta(minutes=5), default_args=default_args)

# define parameters:
dt = datetime.today().strftime("%Y%m%d")
now = datetime.today().strftime("%Y%m%d%H%M%S")
# Beibei: not sure how you calculate the cutoff - please update as required
cutoff = dt + '012959'


# Example of Spark submit with a JAR file

spark_submit_cmd_line = "/usr/lib/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn-client /usr/lib/spark/examples/target/scala-2.11/jars/spark-examples*.jar"
t1 = QuboleOperator(
    task_id='SparkSubmit_example',
    command_type='sparkcmd',
    cmdline=spark_submit_cmd_line,
    dag=dag)

# Example of Spark SQL

insert_runexecution_sql = """
    INSERT INTO TABLE abtest_poc.spark_execution_history
    PARTITION (dt = {{ params.dt }})
    VALUES ({{ params.type }}, '{{ params.subtype }}', '{{ params.timestamp }}', '{{ params.cutoff }}')
"""

t2 = QuboleOperator(
    task_id='SparkSQL_example',
    command_type='sparkcmd',
    sql=insert_runexecution_sql,
    params={'dt': dt,'type': '1', 'subtype': 'start', 'timestamp':now, 'cutoff':cutoff},
    dag=dag)

# Example of Spark Scala inline query example

prog = '''
import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi {
def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
    }
}
'''
# Spark Command - Scala Program
t3 = QuboleOperator(
    task_id='Spark_inline_cmd_example',
    command_type="sparkcmd",
    program=prog,
    language='scala',
    arguments='--class SparkPi',
    dag=dag)


# Example of notebook run from

# Spark Command - Run a Notebook
t4 = QuboleOperator(
     task_id='spark_notebook',
     command_type="sparkcmd",
     note_id="36995",
     qubole_conn_id='qubole_prod',
     arguments='{"name":"hello world"}',
     dag=dag)

# define the flow:

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t2)
