import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.solacesystems.jcsmp.*;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Scanner;

public class WeatherPollutionSubscriber {

    public static JCSMPSession createSession() throws JCSMPException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
        properties.setProperty(JCSMPProperties.USERNAME, solaceConfig.SOLACE_USERNAME);
        properties.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);
        properties.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);
        JCSMPFactory factory = JCSMPFactory.onlyInstance();
        JCSMPSession session = factory.createSession(properties);
        return session;
    }

    public static void main(String[] args) throws JCSMPException {

        try (InfluxDBClient weatherInfluxDBClient = InfluxDBClientFactory.create(influxdbConfig.INFLUXDB_URL, influxdbConfig.INFLUXDB_TOKEN.toCharArray(), influxdbConfig.INFLUXDB_ORG, influxdbConfig.weatherINFLUXDB_BUCKET);
             InfluxDBClient pollutionInfluxDBClient = InfluxDBClientFactory.create(influxdbConfig.INFLUXDB_URL, influxdbConfig.INFLUXDB_TOKEN.toCharArray(), influxdbConfig.INFLUXDB_ORG, influxdbConfig.pollutionINFLUXDB_BUCKET)) {
            // Create JCSMPSessions for each api
            JCSMPSession weatherSession = createSession();
            JCSMPSession pollutionSession = createSession();

            // Create XMLMessageConsumer and configure for OpenWeatherSubscriber
            XMLMessageConsumer weatherSubscriber = weatherSession.getMessageConsumer(new XMLMessageListener() {
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

                    try(WriteApi writeApi = weatherInfluxDBClient.getWriteApi()){
                        writeApi.writePoint(point);
                    }

                }

                @Override
                public void onException(JCSMPException e) {
                    e.printStackTrace();
                }
            });

            // XMLMessageConsumer and configure for AirPollutionSubscriber
            XMLMessageConsumer airPollutionSubscriber = pollutionSession.getMessageConsumer(new XMLMessageListener() {
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
                    try(WriteApi writeApi = pollutionInfluxDBClient.getWriteApi()){
                        writeApi.writePoint(point);
                    }


                }

                @Override
                public void onException(JCSMPException e) {
                    e.printStackTrace();
                }
            });

            // Adding topics to the subscriptions
            Topic weatherTopic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME);
            weatherSession.addSubscription(weatherTopic);

            Topic airPollutionTopic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME1);
            pollutionSession.addSubscription(airPollutionTopic);

            // Starting the consumers
            weatherSubscriber.start();
            airPollutionSubscriber.start();

            // messages wait
            Scanner scanner = new Scanner(System.in);
            System.out.println("Press Enter to exit");
            scanner.nextLine();

            weatherSubscriber.close();
            airPollutionSubscriber.close();
            System.out.println("Exiting.");
            weatherSession.closeSession();
            pollutionSession.closeSession();
        }
    }
}
