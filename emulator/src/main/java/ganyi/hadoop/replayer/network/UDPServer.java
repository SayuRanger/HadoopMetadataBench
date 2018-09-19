package ganyi.hadoop.replayer.network;

import ganyi.hadoop.replayer.message.MessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.*;

/**
 * Created by ganyi on 4/4/2017.
 */
public class UDPServer implements Runnable {
    public static Log LOG = LogFactory.getLog(UDPServer.class);
    DatagramPacket packet = null;
    MessageQueue<String> incomingMessageQueue = null;
    private DatagramSocket sock;
    private InetAddress local;
    private int port;
    private volatile boolean running = false;

    UDPServer(netAddress address, MessageQueue block) {
        port = address.getPort();
        try {
            local = InetAddress.getByName(address.getIp());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        byte[] buffer = new byte[4096];
        packet = new DatagramPacket(buffer, buffer.length);
        incomingMessageQueue = block;
        LOG.info("Initialize UDP server.");
    }

    public void stopServer() {
        running = false;
        sock.close();
    }

    public void run() {
        try {
            this.sock = new DatagramSocket(null);
            sock.bind(new InetSocketAddress(local, port));
        } catch (SocketException e) {
            e.printStackTrace();
        }

        LOG.info("Start Listening.");
        running = true;
        while (running) {
            try {
                sock.receive(packet);
            } catch (IOException e) {
                LOG.info("Socket is closing now.");
                //e.printStackTrace();
                running = false;
                break;
            }

            String receivedMsg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            LOG.info("Receive Message: " + receivedMsg);
            incomingMessageQueue.put(receivedMsg);
        }
        if (!sock.isClosed()) {
            sock.close();
        }
//        running = false;
    }

}
