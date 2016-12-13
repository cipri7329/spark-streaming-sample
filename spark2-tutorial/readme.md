RUN with spark-submit locally

./bin/spark-submit --class "SparkPi" \
    --master local[4] \
    ./target/scala-2.11/spark2-tutorial_2.11-1.0.jar