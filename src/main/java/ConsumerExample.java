import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * Created by korovin on 3/3/2017.
 */
public class ConsumerExample {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();

        // in order to create connection need to specify credentials, address
        // set connection info
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        // TCP connection
        // create the connection
        Connection connection = factory.newConnection();

        // can have multiple channels from one connection
        // create the channel
        Channel channel = connection.createChannel();

        // not event based. can query next message and it will block if no message available
        QueueingConsumer consumer = new QueueingConsumer(channel);
        // attach to channel
        channel.basicConsume("test", consumer);

        boolean removeAllUpTo = true;
        // infinite loop
        while (true) {
            // passing timeout parameter 5 sec, get message
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(5000);
            // (because if there are no messages, break from program
            if (delivery == null) break;
            if (processMessage(delivery)) {
                // if successfully processed message, remove it from queue
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                channel.basicAck(deliveryTag, removeAllUpTo);
            }
        }

        // close all channels and connections
        // first close channels
        channel.close();
        // then close connections
        connection.close();
    }

    private static boolean processMessage(QueueingConsumer.Delivery delivery) throws UnsupportedEncodingException {
        String msg = new String(delivery.getBody(), "UTF-8");
        System.out.println("[x] Recv: redeliver=" + delivery.getEnvelope().isRedeliver() + " , msg=" + msg);
        return false;
    }
}
