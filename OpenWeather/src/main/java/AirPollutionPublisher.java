import com.solacesystems.jcsmp.*;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class AirPollutionPublisher {

    private static final String API_URL = "https://api.openweathermap.org/data/2.5/air_pollution";
    private static final String API_KEY = "afcce688a091e7a3621e0ef65e56ff74";
    private static final String LATITUDE = "-33.868820";
    private static final String LONGITUDE = "151.209290";


    private static final long QUERY_INTERVAL_MS = 60 * 5000;

    public static void main(String[] args) {
        JCSMPSession session = null;

        try {
            JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
            properties.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);
            properties.setProperty(JCSMPProperties.USERNAME, solaceConfig.SOLACE_USERNAME);
            properties.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);

            JCSMPFactory factory = JCSMPFactory.onlyInstance();
            session = factory.createSession(properties);

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

            session.connect();

            while (true){

            String apiUrl = API_URL + "?lat=" + LATITUDE + "&lon=" + LONGITUDE + "&appid=" + API_KEY;

            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();

            String jsonResponse = response.toString();
            JSONObject jsonObject = new JSONObject(jsonResponse);


                String topicName1 = solaceConfig.SOLACE_TOPIC_NAME1;
                Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName1);
                TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                message.setText(jsonObject.toString());

                producer.send(message, topic);
                System.out.println("Message sent to topic " + topic.getName());

                Thread.sleep(QUERY_INTERVAL_MS);
            }

        } catch (IOException | JCSMPException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
