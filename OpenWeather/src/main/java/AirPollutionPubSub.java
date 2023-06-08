import com.solacesystems.jcsmp.JCSMPException;

public class AirPollutionPubSub {
    public static void main(String[] args) {

        Thread publisherThread = new Thread(() -> AirPollutionPublisher.main(args));
        Thread subscriberThread = new Thread(() -> {
            try {
                AirPollutionSubscriber.main(args);
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
