
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.rieter.es.cache.CacheClientStartup;
import com.rieter.es.utils.Constants;
import com.rieter.es.utils.Utilities;
import com.rieter.es.validation.ValidationInitializationException;

import scala.Tuple2;

class CassandraConnectionBroadcast {

	private static volatile Broadcast<CassandraConnector> instance = null;

	public static Broadcast<CassandraConnector> getInstance(JavaSparkContext jsc, SparkConf sparkConf) {
		if (instance == null) {
			synchronized (CassandraConnectionBroadcast.class) {
				if (instance == null) {
					CassandraConnector conn = CassandraConnector.apply(sparkConf);
					instance = jsc.broadcast(conn);
				}
			}
		}
		return instance;
	}
}

public class DataStreamIngestion {

	private static final Utilities util = new Utilities();
	private static final Properties prop = util.loadProperties();
	private static final Logger log = LoggerFactory.getLogger(DataStreamIngestion.class);
	private static Map<String, Integer> map = new HashMap<String, Integer>();
	private static CacheClientStartup clientStartup;

	public static void main(String[] args)
			throws InterruptedException, FileNotFoundException, ValidationInitializationException {

		// get streaming context
		JavaStreamingContext ssc = createContext();

		// start streaming
		ssc.start();

		// wait for streaming to terminate
		ssc.awaitTermination();

	}

	/**
	 * @return
	 * @throws ValidationInitializationException
	 * @throws FileNotFoundException
	 */
	private static JavaStreamingContext createContext()
			throws FileNotFoundException, ValidationInitializationException {

		System.out.println("Creating new context");

		SparkConf sparkConf = new SparkConf().setAppName(prop.getProperty(Constants.APPLICATION_NAME));

		// To Run Spark on Local mode
		sparkConf.setMaster("local[*]");

		// spark cassandra connector setup
		sparkConf.set(prop.getProperty(Constants.SPARK_CASSANDRA_PROPERTY), prop.getProperty(Constants.CASSANDRA_IP));

		// Create the context with a 2 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(2000));

		// Kafka Topic, reading data from kafka topic, sample data:
		// "1,B1243#,10,40,20"
		map.put(prop.getProperty(Constants.KAFKA_TOPIC_NAME), 1);

		// Create Dstream using Kafka createStream to read topic data
		JavaPairReceiverInputDStream<String, String> message = KafkaUtils.createStream(ssc,
				prop.getProperty(Constants.ZOOKEEPER_IP_PORT), "1", map);

		// Get or register the CassandraConnection Broadcast
		final Broadcast<CassandraConnector> broadcastCassandraConnection = CassandraConnectionBroadcast
				.getInstance(new JavaSparkContext(ssc.sparkContext().sc()), ssc.sparkContext().sc().getConf());

		Session session = broadcastCassandraConnection.getValue().openSession();

		// Get data from tuple
		JavaDStream<String> lines = message.map(new Function<Tuple2<String, String>, String>() {
			/**
			 */
			private static final long serialVersionUID = -8029573475841492624L;

			@Override
			public String call(Tuple2<String, String> tuple) throws Exception {
				return tuple._2();
			}

		});

		JavaDStream<String> records = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x).iterator();

			}
		});

		records.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
			private static final long serialVersionUID = -8848777585489054217L;

			@Override
			public void call(JavaRDD<String> rdd, Time time) throws Exception {

				List<String> rows = rdd.collect();

				for (String inputDataStream : rows) {

					String sensorParameters[] = inputDataStream.split(Constants.STREAM_DATA_DELIMITER);

					BoundStatement boundStatement = new BoundStatement(
							session.prepare("insert into sensor values(?,?)"));

					boundStatement.setString("SensorID", "S01");
					boundStatement.setInt("MachineID", 1);

					session.execute(boundStatement);

				}
			}
		});
		records.print();
		return ssc;
	}

}
