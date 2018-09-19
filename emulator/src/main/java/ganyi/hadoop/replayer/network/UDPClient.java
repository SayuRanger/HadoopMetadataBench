package ganyi.hadoop.replayer.network;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.message.MessageUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

/**
 * Created by ganyi on 4/4/2017.
 */
public class UDPClient implements Runnable {
    public static Log LOG = LogFactory.getLog(UDPClient.class);

    GlobalConfigure configure;
    MessageQueue<Message> outboundMessageQueue;
    MessageQueue<String> inboundMessageQueue;
    String simID;

    DatagramSocket sock;
    private volatile boolean running = false;


    UDPClient(MessageQueue<Message> block,
              MessageQueue<String> in,
              GlobalConfigure configure, String id) {
        try {
            sock = new DatagramSocket(null);
            this.outboundMessageQueue = block;
            this.inboundMessageQueue = in;
        } catch (SocketException e) {
            e.printStackTrace();
            sock.close();
        }
        this.configure = configure;
        simID = id;
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            Message message = outboundMessageQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.stopService) {
                running = false;
                break;
            }
            try {
                sendMessage(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        LOG.info("Client is close.");
    }

    public void sendMessage(Message message) throws IOException {
        if (message.getMsgType() == Message.MSG_TYPE.BRD_command) {
            //LOG.info("send BRD message: " + message);
        } else {
            LOG.info("Send message: " + message);
        }
        message.setSrc_pool(simID);
        String msgStr = MessageUtil.encode(message);
        String destSimPool = configure.getSimPoolID(message.getDest());
        if (simID.equals(destSimPool)) {
            inboundMessageQueue.put(msgStr);
            return;
        }
        netAddress addr = configure.getAddr(message.getDest());
        if (addr == null) {
            System.out.println("UDPClient: cannot find such dest<" + message.getDest() + "> in config.");
        }
        InetAddress dest = InetAddress.getByName(addr.getIp());
        int port = addr.getPort();
        sendMessage(msgStr, message.getDest(), dest, port);
    }

    public void sendMessage(String message, String receiver, InetAddress ad, int port) {
        /*if(message.toLowerCase().contains("brd_command")){
            //LOG.info("send BRD message.");
        }
        else {
            LOG.info("send message to " + receiver + "<" + ad.toString() + ": " + port + ">: " + message);
        }*/
        DatagramPacket response = new DatagramPacket(message.getBytes(), message.length(), ad, port);
        try {
            sock.send(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
