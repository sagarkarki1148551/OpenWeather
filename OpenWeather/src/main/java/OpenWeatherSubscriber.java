import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.solacesystems.jcsmp.*;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Scanner;

public class OpenWeatherSubscriber {

    static String urlInflux = "http://localhost:8086";
    static String token = "VbHSHK-nzQZ_nF31xqfPbtg5BADJxP93BrkCKUMIp9MnP1y2K9YZHGkae0siRlY3mDH_tmy0TfQzAjEGevfDhA==";
    static String org = "Urban Institute";
    static String bucket = "Weather";




    public static void main(String[] args) throws JCSMPException {
        // Create a JCSMPSession
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
        properties.setProperty(JCSMPProperties.USERNAME,solaceConfig.SOLACE_USERNAME);
        properties.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);
        properties.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);
        JCSMPFactory factory = JCSMPFactory.onlyInstance();
        JCSMPSession session = factory.createSession(properties);

        // Create a XMLMessageConsumer
        XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage message) {
                TextMessage textMessage = (TextMessage) message;
                String json = textMessage.getText();
                JSONObject jsonObject = new JSONObject(json);
                System.out.println("Received JSON data: " + json.toString());


                Point point = Point.measurement("weather")
                        .addTag("city", jsonObject.getString("name"))
                        //.addField("description", jsonObject.getJSONArray("weather").getJSONObject(0).getString("description"))
                        .addField("temperature", jsonObject.getJSONObject("main").getDouble("temp"))
                        .addField("pressure", jsonObject.getJSONObject("main").getDouble("pressure"))
                        .addField("humidity", jsonObject.getJSONObject("main").getDouble("humidity"))
                        .addField("wind_speed", jsonObject.getJSONObject("wind").getDouble("speed"))
                        .time(Instant.now(), WritePrecision.NS);

                try (InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxdbConfig.INFLUXDB_URL,influxdbConfig.INFLUXDB_TOKEN.toCharArray(), influxdbConfig.INFLUXDB_ORG, influxdbConfig.weatherINFLUXDB_BUCKET);
                     WriteApi writeApi = influxDBClient.getWriteApi()) {
                    writeApi.writePoint(point);

                }



            }


            @Override
            public void onException(JCSMPException e) {
                e.printStackTrace();
            }


        });

        // Add the topic to the subscription
        String topicName = "weather_report";
        Topic topic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME);
        session.addSubscription(topic);

        // Start the consumer
        consumer.start();

        // Wait for messages
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press Enter to exit");
        scanner.nextLine();

        // Stop the consumer
        consumer.close();
        System.out.println("Exiting.");
        session.closeSession();
    }
}
