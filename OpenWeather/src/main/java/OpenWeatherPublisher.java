import com.solacesystems.jcsmp.*;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class OpenWeatherPublisher {

    private static final String API_URL = "https://api.openweathermap.org/data/2.5/weather";
    private static final String API_KEY = "afcce688a091e7a3621e0ef65e56ff74";
    private static final String LATITUDE = "-19.258965";
    private static final String LONGITUDE = "146.816956";

    private static final String UNITS = "metric";

    private static final long QUERY_INTERVAL_MS = 60 * 5000;



    public static void main(String[] args) throws JCSMPException, IOException {

        JCSMPSession session = null;
        try {
            // Create a JCSMPSession
            JCSMPProperties propertiesObj = new JCSMPProperties();
            propertiesObj.setProperty(JCSMPProperties.HOST, solaceConfig.SOLACE_HOST);
            propertiesObj.setProperty(JCSMPProperties.VPN_NAME, solaceConfig.SOLACE_VPN_NAME);
            propertiesObj.setProperty(JCSMPProperties.USERNAME, solaceConfig.SOLACE_USERNAME);
            propertiesObj.setProperty(JCSMPProperties.PASSWORD, solaceConfig.SOLACE_PASSWORD);
            JCSMPFactory factory = JCSMPFactory.onlyInstance();
            session = factory.createSession(propertiesObj);

            // Create a XMLMessageProducer(publisher part)
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

            // Connecting to the Solace event broker
            session.connect();

            while (true){


            // Creates the API URL with query parameters
            String ApiUrl = API_URL + "?lat=" + LATITUDE + "&lon=" + LONGITUDE + "&appid=" + API_KEY + "&units=" +UNITS;

            // Sends an HTTP GET request to the API endpoint
            URL url = new URL(ApiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            // Reads the response from the API
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();

            // Parse the JSON response into a JSONObject
            JSONObject json = new JSONObject(response.toString());

            // Creating a topic and a message to publish
                String topicName = solaceConfig.SOLACE_TOPIC_NAME;
                Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
                TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                message.setText(json.toString());


                // Publishing the message to the topic
                producer.send(message, topic);
                System.out.println("Message sent to topic " + topic.getName());

                // Disconnect from the Solace event broker
                //session.closeSession();

                Thread.sleep(QUERY_INTERVAL_MS);
            }


            }catch (IOException | JCSMPException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

