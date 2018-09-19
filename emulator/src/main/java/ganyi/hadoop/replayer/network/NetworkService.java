package ganyi.hadoop.replayer.network;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class NetworkService implements Runnable {
    public static Log LOG = LogFactory.getLog(NetworkService.class);
    GlobalConfigure configure;
    String simPoolID;
    MessageQueue<Message> inboundQueue;
    MessageQueue<Message> outboundQueue;
    private ServerSocket ss = null;
    private SocketListener socketListener = null;
    private List<Receiver> ReceiverArray = new ArrayList<>();
    private HashMap<InetSocketAddress, ObjectOutputStream> outgoingStreamMap = new HashMap<>();
    private netAddress address;
    private Thread runningThread;
    private boolean running = false;

    public NetworkService(MessageQueue<Message> in, MessageQueue<Message> out,
                          String id, GlobalConfigure configure) {
        inboundQueue = in;
        outboundQueue = out;
        this.configure = configure;
        simPoolID = id;
        this.address = configure.getAddr(simPoolID);
    }

    public void close() {
        for (Receiver receiver : ReceiverArray) {
            receiver.terminate();
            try {
                receiver.runningThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        socketListener.terminate();
        try {
            socketListener.runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        running = false;
        Message msg = new Message(Message.MSG_TYPE.stopService);
        outboundQueue.put(msg);
        try {
            runningThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startService() {
        if (!running) {
            try {
                ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(this.address.toInetSock());
            } catch (IOException e) {
                e.printStackTrace();
            }
            socketListener = new SocketListener(ss);
            socketListener.startListener();
            LOG.info("NetworkService: listening to " + ss + " now.");
            runningThread = new Thread(this);
            runningThread.start();
        }
    }

    private void sendMessage(Message message) {
        message.setSrc_pool(simPoolID);

        String destSimPool = configure.getSimPoolID(message.getDest());
        if (simPoolID.equals(destSimPool)) {
            inboundQueue.put(message);
            return;
        }
        netAddress addr = configure.getAddr(message.getDest());
        if (addr == null) {
            throw new RuntimeException("Cannot find dest <" + message.getDest() + "> in " + GlobalConfigure.class);
        }
        InetSocketAddress receiver = addr.toInetSock();
        ObjectOutputStream out = null;
        //LOG.info("send msg to "+receiver);
        synchronized (outgoingStreamMap) {
            if (outgoingStreamMap.containsKey(receiver)) {
                out = outgoingStreamMap.get(receiver);
            } else {
                Socket s = new Socket();
                try {
                    s.connect(receiver);
                    out = new ObjectOutputStream(s.getOutputStream());
                    outgoingStreamMap.put(receiver, out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                synchronized (out) {
                    try {
                        out.writeObject(message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            synchronized (this) {
                if (!running)
                    return;
            }
            Message msg = outboundQueue.get();
            if (msg.getMsgType() == Message.MSG_TYPE.stopService) {
                running = false;
                break;
            } else {
                sendMessage(msg);
            }
        }
    }

    private class SocketListener implements Runnable {

        protected Thread runningThread = null;
        boolean running = false;
        private ServerSocket sock;

        SocketListener(ServerSocket ss) {
            sock = ss;
        }

        public synchronized void startListener() {
            if (!running) {
                runningThread = new Thread(this);
                runningThread.start();
            }
        }

        public synchronized void terminate() {
            running = false;
            runningThread.interrupt();
            try {
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            running = true;
            while (running) {
                synchronized (this) {
                    if (!running)
                        return;
                }
                try {
                    Socket socket = ss.accept();
                    Receiver receiver = new Receiver(socket);
                    LOG.info("Add new receiver.");
                    ReceiverArray.add(receiver);
                    receiver.startReceiver();
                } catch (IOException e) {
                    LOG.info("SocketListener is closing now.");
                    break;
                }
            }
        }
    }

    private class Receiver implements Runnable {

        protected Thread runningThread = null;
        boolean running = false;
        private Socket sock = null;

        public Receiver(Socket sock) {
            this.sock = sock;
        }

        public synchronized void startReceiver() {
            if (!running) {
                runningThread = new Thread(this);
                runningThread.start();
            }
        }

        public synchronized void terminate() {
            running = false;
            runningThread.interrupt();
            try {
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            running = true;
            ObjectInputStream ois;

            try {
                ois = new ObjectInputStream(sock.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            while (running) {
                synchronized (this) {
                    if (!running) {
                        return;
                    }
                }
                try {
                    Message msg = (Message) ois.readObject();
                    inboundQueue.put(msg);
                } catch (IOException e) {
                    //e.printStackTrace();
                    /*System.out.println("Error in network service "+address);
                    e.printStackTrace();*/
                    //LOG.info("Receiver channel is closing.");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }
}
