import com.solacesystems.jcsmp.JCSMPException;

import java.io.IOException;

public class OpenWeatherMapPubSub {
    public static void main(String[] args) {

        Thread publisherThread = new Thread(() -> {
            try {
                OpenWeatherPublisher.main(args);
            } catch (JCSMPException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread subscriberThread = new Thread(() -> {
            try {
                OpenWeatherSubscriber.main(args);
            } catch (JCSMPException e) {
                throw new RuntimeException(e);
            }
        });

        publisherThread.start();
        subscriberThread.start();


        try {
            publisherThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
