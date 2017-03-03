import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by korovin on 3/3/2017.
 */
public class PublisherExample {
    public static void main(String[] args) throws IOException, TimeoutException {
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

        // publish 10 messages to queue
        String queueName = "test";
        for (int i = 1; i <= 10; i++) {
            String message = "Hello world, message #" + i;
            // in order to publish a message, use basicPublish method
            // specified empty exchange, it is mapped to default exchange
            channel.basicPublish("", queueName, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        } // 10 messages are ready for consuming. they stay in the queue, until they are consumed

        // close all channels and connections
        // first close channels
        channel.close();
        // then close connections
        connection.close();
    }
}
