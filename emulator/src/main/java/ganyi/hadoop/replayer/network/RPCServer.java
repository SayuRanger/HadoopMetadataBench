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

public class RPCServer implements Runnable {
    public static Log LOG = LogFactory.getLog(RPCServer.class);

    ServerSocket ss;
    netAddress address = null;
    GlobalConfigure configure;
    MessageQueue<Message> inboundQueue;
    MessageQueue<Message> outboundQueue;
    private TCPServer server;
    private List<Receiver> receiverList = new ArrayList<>();
    private HashMap<String, Receiver> outputMap = new HashMap<>();
    private volatile boolean running = false;
    private Thread runningThread = null;

    public RPCServer(MessageQueue<Message> in, MessageQueue<Message> out,
                     GlobalConfigure configure) {
        inboundQueue = in;
        outboundQueue = out;
        this.configure = configure;

        address = configure.getAddr("rpc.1");
    }

    public RPCServer(GlobalConfigure configure) {
        this.configure = configure;
        address = configure.getAddr("rpc.1");
        inboundQueue = new MessageQueue<>();
        outboundQueue = new MessageQueue<>();
    }

    public netAddress getAddress() {
        return address;
    }

    public synchronized void startService() {
        if (!running) {
            try {
                ss = new ServerSocket();
                ss.bind(address.toInetSock());
            } catch (IOException e) {
                e.printStackTrace();
            }
            server = new TCPServer(ss);
            server.startServer();
            runningThread = new Thread(this);
            runningThread.start();
        }
    }

    public synchronized void close() {
        running = false;

        server.terminate();
        try {
            server.runningThread.join();
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

    void sendMessage(Message msg) {
        netAddress addr = new netAddress(msg.getDest());
        LOG.info("send message to " + addr);
        InetSocketAddress receiver = addr.toInetSock();
        Receiver out = null;
        synchronized (outputMap) {
            if (outputMap.containsKey(msg.getDest())) {
                out = outputMap.get(msg.getDest());
                synchronized (out) {
                    try {
                        ObjectOutputStream oos = new ObjectOutputStream(out.sock.getOutputStream());
                        oos.writeObject(msg.getCmd().toUpperCase());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                //error
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

    private class TCPServer implements Runnable {

        protected Thread runningThread = null;
        volatile boolean running = false;
        ServerSocket ss;

        TCPServer(ServerSocket ss) {
            this.ss = ss;
        }

        public synchronized void startServer() {
            if (!running) {
                runningThread = new Thread(this);
                runningThread.start();
                InetSocketAddress saddr = (InetSocketAddress) ss.getLocalSocketAddress();
                LOG.info("start listening socket: " + saddr.getAddress().getHostAddress() + ":" + saddr.getPort());
            }
        }

        public synchronized void terminate() {
            running = false;
            try {
                ss.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            runningThread.interrupt();

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
                    LOG.info("Accept new connection, from " + socket.getRemoteSocketAddress());
                    Receiver receiver = new Receiver(socket);
                    receiverList.add(receiver);
                    receiver.startReceiver();
                } catch (IOException e) {
                    LOG.info("ServerSocket is closing.");
                    break;
                }
            }
        }

    }

    private class Receiver implements Runnable {

        protected Thread runningThread = null;
        volatile boolean running = false;
        private Socket sock = null;
        /*private String id;*/

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
                    String str = (String) ois.readObject();

                    /**
                     *  application attempt id format:
                     *  appattempt_1515558297929_0002_000001
                     *  <prefix>_<cluster_time_stamp>_<app_id>_<attempt_id>
                     */

                    Message msg = new Message(Message.MSG_TYPE.RM_RPC_CALL);
                    InetSocketAddress saddr = (InetSocketAddress) sock.getRemoteSocketAddress();
                    String src = saddr.getAddress().getHostAddress() + ":" + saddr.getPort();
                    msg.setRMRPC(str, src);

                    LOG.info("read object: " + msg);

                    outputMap.put(src, this);
                    inboundQueue.put(msg);
                } catch (IOException e) {
                    LOG.info("Connection is closed from client side.");
                    receiverList.remove(this);
                    running = false;
                    break;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }
}
