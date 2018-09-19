package ganyi.hadoop.replayer.simulator;


import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.NetworkService;
import ganyi.hadoop.replayer.simulator.nn.ClientSimulator;
import ganyi.hadoop.replayer.simulator.nn.DataNodeSimulator;
import ganyi.hadoop.replayer.simulator.nn.SharedCondCounter;
import ganyi.hadoop.replayer.simulator.rm.NodeManagerSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerAppMasterSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerClientSimulator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.util.HashMap;

import static ganyi.hadoop.replayer.message.Message.MSG_TYPE.batch_processing;
import static ganyi.hadoop.replayer.message.Message.MSG_TYPE.nm_message;
import static ganyi.hadoop.replayer.message.Message.MSG_TYPE.sync_dn;


public class SimulatorPool implements Runnable {
    public static Log LOG = LogFactory.getLog(SimulatorPool.class);
    String[] envs;
    /*UDPServer server;
    Thread serverThread;
    Thread clientThread;*/
    NetworkService tcpService;
    HashMap<String, Thread> threadPool;
    GlobalConfigure configure;

    HashMap<String, MessageQueue<Message>> queuePool;
    HashMap<String, SharedCondCounter> counterHashMap;
    MessageQueue<Message> inboundMessageQueue;
    MessageQueue<Message> outboundMessageQueue;

    String SimPoolID;
    Integer nmport = 0;

    Integer getNmport(){
        synchronized (nmport) {
            nmport += 1;
            return nmport;
        }
    }

    long count = 0;

    public SimulatorPool(String[] args) {
        setEnvs(args);
    }

    public void setEnvs(String[] envs) {
        this.envs = envs;
    }

    boolean processMessage() {
        boolean retVal = true;
        count += 1;

        Message msg = inboundMessageQueue.get();
        if (count % 1000 == 0) {
            LOG.info("Receive 1000 Messages.");
        }
        //GYF
        /*if (msg.getMsgType() != Message.MSG_TYPE.BRD_command) {
            LOG.info("Current message in process is " + msg);
        }*/
        if (msg.getMsgType() == Message.MSG_TYPE.command) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            if (ref == null) {
                System.out.printf("Fail to find value by key: %s\n", id);
                return false;
            }
            ref.put(msg);
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.start_HB_count) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            if (ref == null) {
                System.out.printf("Fail to find value by key: %s\n", id);
                return false;
            }
            ref.put(msg);
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.create_simulator) {
            retVal = createThread(msg);
        } else if (msg.getMsgType() == Message.MSG_TYPE.release_simulator) {
            retVal = releaseThread(msg);
        } else if (msg.getMsgType() == Message.MSG_TYPE.stop_instance) {
            stopService(msg.getJobID());
            retVal = false;
        } else if (msg.getMsgType() == Message.MSG_TYPE.BRD_command) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            if (ref == null) {
                System.out.printf("Fail to find value by key: %s\n", id);
                return false;
            }
            ref.put(msg);
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.ChangeHBState) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            if (ref == null) {
                System.out.printf("Fail to find value by key: %s\n", id);
                return false;
            }
            ref.put(msg);
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.BRD_finish) {
            String id = msg.getCmd().split("#")[1];
            SharedCondCounter condCounter = counterHashMap.get(id);
            condCounter.dec();
            //System.out.println("dec on "+ msg.getCmd());
            /*System.out.println("Receive BRD_finish from "+msg.getSrc()+ " at " +
                    msg.getSrc_pool()+ " to "+ id);*/
            retVal = true;
        } else if (msg.getMsgType() == nm_message) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            ref.put(msg);
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.sync_dn) {
            String cmd = msg.getCmd();
            configure.parseSimulatorGenString(cmd, "dn");
            configure.showConfig();
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.sync_nm) {
            String cmd = msg.getCmd();
            configure.parseSimulatorGenString(cmd, "nm");
            configure.showConfig();
            retVal = true;
        } else if (msg.getMsgType() == Message.MSG_TYPE.RMAppMasterStatus) {
            String id = msg.getDest();
            MessageQueue<Message> ref = queuePool.get(id);
            ref.put(msg);
            retVal = true;
        } else if(msg.getMsgType() == batch_processing){
            String cmd = msg.getCmd();
            if (cmd.equalsIgnoreCase("start_container")){
                batch_start_container(msg);
            }
            else if(cmd.equalsIgnoreCase(Message.MSG_TYPE.ChangeHBState.name())){
                batch_Change_state(msg);
            }
            else{
                //Error.
            }
            System.gc();
            retVal = true;
        }
        else {
            throw new RuntimeException("Unknown msg type: " + msg.getMsgType().name() + "\nMessage: " + msg);
        }
        return retVal;
    }

    void batch_start_container(Message msg){
        for (String s: msg.getMisc().split("\\|")){
            String[] tuple = s.split(":");
            String simID = tuple[0];
            String time = tuple[1];
            String containerId = tuple[2];

            Message message = new Message(Message.MSG_TYPE.nm_message, "start_container",
                    msg.getSrc(),simID,msg.getJobID());
            message.setMisc(time+":"+containerId);
            queuePool.get(simID).put(message);
        }
        /*try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    void batch_Change_state(Message msg){
        for (String s: msg.getMisc().split("\\|")){
            String[] tuple = s.split(":");
            String simID = tuple[0];
            String state = tuple[1];

            Message message = new Message(Message.MSG_TYPE.ChangeHBState,
                    state,msg.getSrc(),simID,msg.getJobID());
            queuePool.get(simID).put(message);
        }
    }

    boolean createThread(Message msg) {
        String id = msg.getCmd();
        long jobID = msg.getJobID();
        Message message = new Message(Message.MSG_TYPE.create_simulator,
                id, this.SimPoolID, configure.getCCID(), jobID);
        if (threadPool.containsKey(id)) {
            LOG.error("Key already exists in threadPool.\n");
            return false;
        }
        if (queuePool.containsKey(id)) {
            LOG.error("Key already exists in queuePool.\n");
            return false;
        }
        MessageQueue<Message> newIncomingQueue = new MessageQueue<>();
        LOG.info("Create simulator with id <"+id+">.");
        if (msg.getTaskType() == Message.TaskType.NameNode) {
            if (id.toLowerCase().contains("dn")) {
                Integer integer = Integer.parseInt(id.split("\\.")[1]);
                DataNodeSimulator simulator =
                        new DataNodeSimulator(new String[]{id},
                                configure, integer * 1 + 10005,
                                newIncomingQueue, outboundMessageQueue);
                simulator.setJobID(jobID);
                threadPool.put(id, new Thread(simulator));
                queuePool.put(id, newIncomingQueue);
                configure.updateSimulatorMap(id, SimPoolID);
            } else {
                ClientSimulator simulator = new ClientSimulator(new String[]{id,msg.getMisc()},
                        configure, newIncomingQueue, outboundMessageQueue);
                SharedCondCounter condCounter = new SharedCondCounter();
                simulator.setBRDCounter(condCounter);
                simulator.setJobID(jobID);
                counterHashMap.put(id, condCounter);
                MessageQueue<Message> receiveQueue = simulator.getInboundQueue();
                threadPool.put(id, new Thread(simulator));
                queuePool.put(id, receiveQueue);
                configure.updateSimulatorMap(id, SimPoolID);
            }
        } else if (msg.getTaskType() == Message.TaskType.ResourceManager) {
            if (id.toLowerCase().contains("nm")) {
                int port = getNmport();
                String host = configure.getLocalInetAddress().getHostName()+":"+String.valueOf(port);
//                Integer integer = Integer.parseInt(id.split("\\.")[1]);
                NodeManagerSimulator simulator =
                        new NodeManagerSimulator(new String[]{id},
                                configure, port,
                                newIncomingQueue, outboundMessageQueue);
                simulator.setJobID(jobID);
                threadPool.put(id, new Thread(simulator));
                queuePool.put(id, newIncomingQueue);
                configure.updateSimulatorMap(id, SimPoolID);
                message.setMisc(host);
            } else if (id.toLowerCase().contains("am")) {
                int totalRequest = Integer.valueOf(msg.getMisc().split("\\|")[0]);
                String token = msg.getMisc().split("\\|")[1];
                ResourceManagerAppMasterSimulator simulator =
                        new ResourceManagerAppMasterSimulator(
                                new String[]{
                                        id,
                                        String.valueOf(totalRequest),
                                        token,
                                        msg.getMisc().split("\\|")[2]
                                },
                                configure,
                                newIncomingQueue,
                                outboundMessageQueue);
                simulator.setJobID(jobID);
                threadPool.put(id, new Thread(simulator));
                queuePool.put(id, newIncomingQueue);
                configure.updateSimulatorMap(id, SimPoolID);
            } else if (id.toLowerCase().contains("run")) {
                ResourceManagerClientSimulator simulator =
                        new ResourceManagerClientSimulator(new String[]{id},
                                configure, newIncomingQueue, outboundMessageQueue);
                simulator.setJobID(jobID);
                threadPool.put(id, new Thread(simulator));
                queuePool.put(id, newIncomingQueue);
                configure.updateSimulatorMap(id, SimPoolID);
            } else {
                throw new RuntimeException("Unable to recognize id <" + id + ">.");
            }
        } else {
            //Error
            throw new RuntimeException("Unable to recognize message taskType: " + msg.getTaskType());

        }
        threadPool.get(id).start();
        //Reply with 'start finished'.

        outboundMessageQueue.put(message);
        return true;
    }

    boolean releaseThread(Message msg) {
        String id = msg.getCmd();
        Thread threadToBeDeleted = threadPool.get(id);
        LOG.info("Release simulator with id <"+id+">.");
        if (threadToBeDeleted == null) {
            System.out.printf("Error: cannot find thread by key: %s\n", id);
            return false;
        }
        MessageQueue<Message> refqueue = queuePool.get(id);
        if (refqueue == null) {
            System.out.printf("Fail to find value by key: %s\n", id);
            return false;
        }
        Message stopMsg = new Message(Message.MSG_TYPE.release_simulator);
        refqueue.put(stopMsg);
        /*try {
            threadToBeDeleted.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        wipeRecord(id);
        Message message = new Message(Message.MSG_TYPE.release_simulator, id,
                this.SimPoolID, configure.getCCID(), msg.getJobID());
        outboundMessageQueue.put(message);
        return true;
    }

    void wipeRecord(String id) {
        threadPool.remove(id);
        queuePool.remove(id);
        configure.deleteSimulatorMapEntry(id);
    }

    @Override
    public void run() {
        LOG.info(this.getClass() + " init.");
        init();
        LOG.info(this.getClass() + " start.");
        while (processMessage()) ;
        LOG.info("SimulatorPool exit.");
    }

    void init() {
        System.out.println("Env: " + envs[0] + "," + envs[1]);
        String configFile = envs[0];
        SimPoolID = envs[1];

        configure = new GlobalConfigure(configFile, SimPoolID);

        inboundMessageQueue = new MessageQueue<>();
        outboundMessageQueue = new MessageQueue<>();

        tcpService = new NetworkService(inboundMessageQueue, outboundMessageQueue, SimPoolID, configure);
        /*serverThread = new Thread(server = new UDPServer(configure.getAddr(SimPoolID),
                inboundMessageQueue));
        clientThread = new Thread(new UDPClient(outboundMessageQueue,
                inboundMessageQueue,configure,SimPoolID));*/


        threadPool = new HashMap<>();
        queuePool = new HashMap<>();

        tcpService.startService();
        /*clientThread.start();
        serverThread.start();*/

        counterHashMap = new HashMap<>();
        nmport=10000 + Integer.valueOf(envs[2]);
        LOG.info("init() finished.");
    }

    void stopAllSimulator(long jobID) throws InterruptedException {
        String[] keys = threadPool.keySet().toArray(new String[threadPool.keySet().size()]);
        Message stopMsg = new Message(Message.MSG_TYPE.release_simulator);
        for (String key : keys) {
            MessageQueue<Message> refQueue = queuePool.get(key);
            refQueue.put(stopMsg);

        }
        Thread.sleep(5000);
        for(String key: keys){
            Thread refThread = threadPool.get(key);
            refThread.join();
            wipeRecord(key);
        }
        /*Message stopMessage = new Message(Message.MSG_TYPE.stop_instance,
                this.SimPoolID,this.SimPoolID,configure.getCCID(),jobID);
        outboundMessageQueue.put(stopMessage);*/
    }

    void stopService(long jobID) {
        try {
            stopAllSimulator(jobID);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopService();
        /*try {
            clientThread.join();
            serverThread.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }*/
    }

    private void stopService() {
        tcpService.close();
    }

   /* public static void main(String[] args){
        Thread thread = new Thread(new SimulatorPool(args));
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }*/
}
