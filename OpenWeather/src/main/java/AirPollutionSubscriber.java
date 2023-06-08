import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.solacesystems.jcsmp.*;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Scanner;

public class AirPollutionSubscriber {
//    static String urlInflux = "http://localhost:8086";
//    static String token = "VbHSHK-nzQZ_nF31xqfPbtg5BADJxP93BrkCKUMIp9MnP1y2K9YZHGkae0siRlY3mDH_tmy0TfQzAjEGevfDhA==";
//    static String org = "Urban Institute";
//    static String bucket = "AirPollution";

    public static void main(String[] args) throws JCSMPException {
        JCSMPSession session = null;

            JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
            properties.setProperty(JCSMPProperties.USERNAME,solaceConfig.SOLACE_USERNAME);
            properties.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);
            properties.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);

            JCSMPFactory factory = JCSMPFactory.onlyInstance();
            session = factory.createSession(properties);

            XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage message) {
                    String json = ((TextMessage) message).getText();
                    System.out.println("Received Json Data: " + json);
                    JSONObject jsonObject = new JSONObject(json);
                    double latitude = jsonObject.getJSONObject("coord").getDouble("lat");
                    double longitude = jsonObject.getJSONObject("coord").getDouble("lon");
                    JSONObject components = jsonObject.getJSONArray("list").getJSONObject(0).getJSONObject("components");


                Point point = Point.measurement("air_pollution")
                        .addTag("latitude", String.valueOf(latitude))
                        .addTag("longitude", String.valueOf(longitude))
                        .addField("co", components.getDouble("co"))
                        .addField("no", components.getDouble("no"))
                        .addField("no2", components.getDouble("no2"))
                        .addField("o3", components.getDouble("o3"))
                        .addField("so2", components.getDouble("so2"))
                        .addField("nh3", components.getDouble("nh3"))
                        .time(Instant.now(), WritePrecision.NS);

                    try(InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxdbConfig.INFLUXDB_URL,influxdbConfig.INFLUXDB_TOKEN.toCharArray(), influxdbConfig.INFLUXDB_ORG, influxdbConfig.pollutionINFLUXDB_BUCKET);
                WriteApi writeApi = influxDBClient.getWriteApi())

                {
                    writeApi.writePoint(point);

                }
            }


                @Override
                public void onException(JCSMPException e) {
                    e.printStackTrace();
                }
            });

            String topicName1 = solaceConfig.SOLACE_TOPIC_NAME1;
            Topic topic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME1);
            session.addSubscription(topic);

            session.connect();
            consumer.start();

            Scanner scanner = new Scanner(System.in);
            System.out.println("Press Enter to exit");
            scanner.nextLine();

            consumer.close();
            System.out.println("Exiting.");
            session.closeSession();

        }
    }
