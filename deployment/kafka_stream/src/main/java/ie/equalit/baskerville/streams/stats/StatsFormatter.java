package ie.equalit.baskerville.streams.stats;

import ie.equalit.baskerville.streams.stats.serde.JsonDeserializer;
import ie.equalit.baskerville.streams.stats.serde.JsonSerializer;
import ie.equalit.baskerville.streams.stats.serde.WrapperSerde;
import ie.equalit.baskerville.streams.stats.model.Weblog;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Properties;

/**
 * Input is a stream of trades
 * Output is two streams: One with minimum and avg "ASK" price for every 10 seconds window
 * Another with the top-3 stocks with lowest minimum ask every minute
 */
public class StatsFormatter{

    public static void main(String[] args) throws Exception {

//         Properties props;
//         if (args.length==1)
//             props = LoadConfigs.loadConfig(args[0]);
//         else
//             props = LoadConfigs.loadConfig();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "baskerville-stats");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WeblogSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
//         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating an AdminClient and checking the number of brokers in the cluster, so I'll know how many replicas we want...

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor",clusterSize);
        else
            props.put("replication.factor",3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Weblog> source = builder.stream(Constants.WEBLOG_TOPIC);

        KStream<String, Weblog> stats = source
                .mapValues((weblog) -> weblog.correct());

        stats.to("STATS_WEBLOGS_5M_CORRECTED");

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    static public final class WeblogSerde extends WrapperSerde<Weblog> {
        public WeblogSerde() {
            super(new JsonSerializer<Weblog>(), new JsonDeserializer<Weblog>(Weblog.class));
        }
    }

}
