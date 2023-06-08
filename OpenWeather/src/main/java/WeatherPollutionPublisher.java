import com.solacesystems.jcsmp.*;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WeatherPollutionPublisher {

    private static final String API_URL_WEATHER = "https://api.openweathermap.org/data/2.5/weather";
    private static final String API_URL_AIR = "http://api.openweathermap.org/data/2.5/air_pollution";
    private static final String API_KEY = "afcce688a091e7a3621e0ef65e56ff74";
    private static final String LATITUDE = "-26.650000";
    private static final String LONGITUDE = "153.066666";
    private static final String UNITS = "metric";

    private static final long QUERY_INTERVAL_MS = 60 * 5000;

    public static void main(String[] args) throws JCSMPException, IOException {

        // Create Solace session properties
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
        properties.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);
        properties.setProperty(JCSMPProperties.USERNAME, solaceConfig.SOLACE_USERNAME);
        properties.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);

        // Create Solace session and connect to the event broker
        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // Create Solace message producer
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                System.out.println("Response received for message: " + messageID);
            }

            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.out.println("Error occurred for message: " + messageID);
                e.printStackTrace();
            }
        });

        // Create and start the OpenWeather publisher thread
        Thread weatherThread = new Thread(() -> {
            try {
                while (true) {
                    // Build the API URL for OpenWeather
                    String apiUrl = API_URL_WEATHER + "?lat=" + LATITUDE + "&lon=" + LONGITUDE + "&appid=" + API_KEY + "&units=" + UNITS;

                    // Send an HTTP GET request to the OpenWeather API
                    URL url = new URL(apiUrl);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");

                    // Read the response from the API
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    reader.close();

                    // Parse the JSON response into a JSONObject
                    JSONObject json = new JSONObject(response.toString());

                    // Create the Solace topic and message to publish

                    Topic weatherTopic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME);
                    TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                    message.setText(json.toString());

                    // Publish the message to the Solace event broker
                    producer.send(message, weatherTopic);

                    // Wait for the next query interval
                    Thread.sleep(QUERY_INTERVAL_MS);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        weatherThread.start();

        // Create and start the AirPollution publisher thread

        Thread PollutionThread = new Thread(() -> {
            try {
                while (true) {
                    // Build the API URL for OpenWeather
                    String apiUrl = API_URL_AIR + "?lat=" + LATITUDE + "&lon=" + LONGITUDE + "&appid=" + API_KEY;

                    // Send an HTTP GET request to the Pollution API
                    URL url = new URL(apiUrl);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");

                    // Read the response from the API
                    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    reader.close();

                    // Parse the JSON response into a JSONObject
                    JSONObject json = new JSONObject(response.toString());

                    // Create the Solace topic and message to publish

                    Topic airPollutionTopic = JCSMPFactory.onlyInstance().createTopic(solaceConfig.SOLACE_TOPIC_NAME1);
                    TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                    message.setText(json.toString());

                    // Publish the message to the Solace event broker
                    producer.send(message, airPollutionTopic);

                    // Wait for the next query interval
                    Thread.sleep(QUERY_INTERVAL_MS);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        PollutionThread.start();
    }
}
