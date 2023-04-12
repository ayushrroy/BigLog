// Producer: a simple Kafka Producer that takes the lines of a file, parses them into a map and sends the parsed data to the topic
// Authors: Sarthak Banerjee, Ajinkya Fotear, James Hinton, Vinayak Kumar, Sydney May, and Ayush Roy
// Version: v0 (created on 4/11/2023)

// TODO: not working, tried to create maven pom.xml file, but kind of confused
//<dependency>
//	<groupId>org.apache.kafka</groupId>
//	<artifactId>kafka-clients</artifactId>
//	<version>3.4.0</version>
//</dependency>

import java.util.Properties

// TODO: getting error that apache is not member of org. This is because dependancy is not working.
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Kafka_Producer{

    // creates a map of the parts of a line read in from a file
    def to_map(line:String) = {
        val line_map = Map[String, String]();
        val line_arr = line.split("\"");
        val first_part = line_arr(0).split(" ")
        val second_part = line_arr(1).split(" ")
        val third_part = line_arr(2).split(" ")

        line_map + ("ip_address" -> first_part(0));
        line_map + ("date_time" -> (first_part(3).substring(1) + " " + first_part(4).substring(0, first_part(4).size - 1)));
        line_map + ("request_type" -> second_part(0));
        line_map + ("request_arg" -> second_part(1));
        line_map + ("status_code" -> third_part(1));
        line_map + ("response_size" -> third_part(2));
        line_map + ("referrer" -> line_arr(3));
        line_map + ("user_agent" -> line_arr(5));
    }

    // reads the lines from a file, parses them, and sends the parsed data to topic_test
    def stream_file_lines(filename:String, producer:KafkaProducer) = {
        val file = scala.io.Source.fromFile(filename).getLines
        for (line <- file) {
            val ip = line.split(" ")(0);
            producer.send(new ProducerRecord<String, String>("topic_test", to_map(line)))
            println(s"Sent $line to topic_test");
        }
    }

    // defines the properties of the kafka producer
    val props:Properties = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    // TODO: need to fix to use lamda correctly in scala
    // TODO: also need scala version of json dumps
    //props.put("key.serializer", lambda x: dumps(x).encode('utf-8'));
    //props.put("value.serializer", lambda x: x.encode('utf-8'));

    // initialization of the kafka producer
    val producer = new KafkaProducer[String, String](props);

    // send lines to topic
    stream_file_lines("archive/short.access.log", producer);

}
