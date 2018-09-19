package ganyi.hadoop.replayer.controller.common;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.controller.NameNodeTaskExecutor;
import ganyi.hadoop.replayer.controller.ResourceManagerTaskExecutor;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.NetworkService;
import ganyi.hadoop.replayer.network.RPCServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ganyi on 4/17/2017.
 */
public class CentralController implements Runnable {
    public static Log LOG = LogFactory.getLog(CentralController.class);

    String[] envs;
    GlobalConfigure configure;
    ConcurrentHashMap<Long, Thread> jobExecutorMap;
    HashMap<Long, MessageQueue<Message>> jobInboundMessageMap;
    JobScheduler scheduler;
    MessageQueue<Message> inboundQueue;
    MessageQueue<Message> outboundTCPQueue;
    MessageQueue<Message> outboundRPCQueue;
    NetworkService tcpService;
    long jobID = 0;
    HashMap<TaskID, Long> taskMapping;
    RPCServer rpcServer;
    private String identifier;
    private MessageQueue<Message> incomingMsgQueue;
    HashMap<String, String> NodeManagerMapping;

    public void setNodeManagerMapping(String inet, String id){
        NodeManagerMapping.put(inet,id);
    }

    public String getNodeManagerMapping(String inet){
        return NodeManagerMapping.get(inet);
    }

    public CentralController(String[] args) {
        this.envs = args;
    }

    public MessageQueue<Message> getIncomingMsgQueue() {
        return incomingMsgQueue;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void init() {
        String configFile = envs[0];
        String id = envs[1];
        String jobListFile = envs[2];
        setIdentifier(id);
        jobID = System.currentTimeMillis();

        configure = new GlobalConfigure(configFile);


        inboundQueue = new MessageQueue<>();
        outboundTCPQueue = new MessageQueue<>();

        tcpService = new NetworkService(inboundQueue, outboundTCPQueue, getIdentifier(), configure);
        tcpService.startService();

        outboundRPCQueue = new MessageQueue<>();
        rpcServer = new RPCServer(inboundQueue, outboundRPCQueue, configure);
        if(rpcServer.getAddress() !=null) {
            rpcServer.startService();
        }
        else{
            rpcServer = null;
        }

        jobInboundMessageMap = new HashMap<>();
        incomingMsgQueue = new MessageQueue<>();
        jobInboundMessageMap.put(jobID, incomingMsgQueue);
        taskMapping = new HashMap<>();

        NodeManagerMapping = new HashMap<>();
        scheduler = new JobScheduler(jobListFile, this);
    }

    void runNNReplay() {
        LOG.info("Setup datanode.");
        scheduler.setupDataNode();

        Thread jobExeManager = new Thread(() -> {
            while (JobExecutorHandler()) ;
        });
        jobExeManager.start();
        String dnwait = configure.getSetting("dnwait", "5000");
        try {
            Thread.sleep(Long.valueOf(dnwait));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long t1 = System.currentTimeMillis();
        initJobExecutor();
        try {
            jobExeManager.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: " + (t2 - t1));
    }

    void runRMReplay() {
        LOG.info("Setup node manager.");
        scheduler.setupNodeManager();
        Thread jobExeManager = new Thread(() -> {
            while (JobExecutorHandler()) ;
        });
        jobExeManager.start();

        long t1 = System.currentTimeMillis();
        initJobExecutor();
        try {
            jobExeManager.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: " + (t2 - t1));

    }

    @Override
    public void run() {
        init();

        Thread dispatchThread = new Thread(() -> MessageDispatch());
        dispatchThread.start();

        if (envs[3].equalsIgnoreCase("nn")) {
            runNNReplay();
        } else if (envs[3].equalsIgnoreCase("rm")) {
            runRMReplay();
        }

        for (String dest : configure.getSimulatorPoolSet()) {
            Message msg = new Message(Message.MSG_TYPE.stop_instance,
                    "_", getIdentifier(), dest, jobID);
            outboundTCPQueue.put(msg);
        }
        Message stopDispatchMessage = new Message(Message.MSG_TYPE.break_Message_dispatch,
                "_", getIdentifier(), getIdentifier(), jobID);
        outboundTCPQueue.put(stopDispatchMessage);
        try {
            dispatchThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopRPCService();
        stopTCPService();
    }

    long[] genJobID(int size){
        long[] longs = new long[size];
        for (int i=0;i< size;i++){
            longs[i] = 0L+ (long)(Math.random()*(1L << 32));
        }

        Arrays.sort(longs);
        for(int i=0;i<longs.length-1;i++){
            if(longs[i] == longs[i+1]){
                longs[i+1] +=1;
            }
        }

        return longs;
    }

    /**
     * Initialize jobExe and assign job script/folder to jobExe.
     */
    void initJobExecutor() {
        jobExecutorMap = new ConcurrentHashMap<>();
        //Launch jobExecutor.
        String str;
        Random random = new Random(System.currentTimeMillis());
        int total = scheduler.getTotaljob();
        //System.out.println("Total job number: "+total);
        long[] jobIDList = genJobID(total);

        int index = 0;
        while (!(str = scheduler.getNextJob()).equals("")) {
            long jobid = jobIDList[index++];
            String[] ss = str.split("#");
            MessageQueue<Message> inQueue = new MessageQueue<>();
            if (ss[2].equalsIgnoreCase("nn")) {
                TaskExecutor executor = new NameNodeTaskExecutor(
                        ss, inQueue, outboundTCPQueue, this
                );
                executor.setJobID(jobid);
                //long jobid = executor.getJobID();
                jobExecutorMap.put(jobid, new Thread(executor));
                jobInboundMessageMap.put(jobid, inQueue);
            } else if (ss[2].equalsIgnoreCase("rm")) {
                TaskExecutor executor = new ResourceManagerTaskExecutor(
                        ss, inQueue, outboundTCPQueue, this
                );
                executor.setJobID(jobid);
                //long jobid = executor.getJobID();
                jobExecutorMap.put(jobid, new Thread(executor));
                jobInboundMessageMap.put(jobid, inQueue);
            }
        }
        String jobinterval = configure.getSetting("jobinterval");
        long interval = jobinterval != null ? Long.valueOf(jobinterval) : 0;
        Iterator<Map.Entry<Long, Thread>> it = jobExecutorMap.entrySet().iterator();
        while(it.hasNext()){
            if(interval > 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Map.Entry<Long, Thread> item = it.next();
            item.getValue().start();
        }

    }



    public void sendTCPMessage(Message msg) {
        outboundTCPQueue.put(msg);
    }

    public void sendRPCMessage(Message msg) {
        outboundRPCQueue.put(msg);
    }

    /**
     * Dispatch message received from UDPServer to designated jobExecutor.
     */
    void MessageDispatch() {
        while (true) {
            Message msg = inboundQueue.get();
            if (msg.getMsgType() == Message.MSG_TYPE.break_Message_dispatch) {
                break;
            } else if (msg.getMsgType() == Message.MSG_TYPE.obtain_appid) {
                LOG.info("obtain_appid: " + msg);
                String cmd = msg.getCmd();
                String[] ss = cmd.split(":"); // cmd: "id:ts"
                TaskID taskId = new TaskID(Integer.valueOf(ss[0]), Long.valueOf(ss[1]));
                taskMapping.put(taskId, msg.getJobID());
            } else if (msg.getMsgType() == Message.MSG_TYPE.RM_RPC_CALL) {
                String cmd = msg.getCmd();
                String[] tuple = cmd.split("\\|");
                String[] ss = tuple[1].split("_");

                TaskID taskID = new TaskID(Integer.valueOf(ss[2]), Long.valueOf(ss[1]));
                if (taskMapping.containsKey(taskID)) {
                    String inet = tuple[2];
                    long jobID = taskMapping.get(taskID);
                    msg.setJobID(jobID);
                    msg.setMisc(inet);
                    jobInboundMessageMap.get(jobID).put(msg);
                } else {
                    LOG.error("cannot find such key(taskID): " + taskID);
                }
            } else {
                long jobid = msg.getJobID();
                LOG.info("Dispatch message <" + msg + "> to jobID " + jobid);
                jobInboundMessageMap.get(jobid).put(msg);
            }
        }
    }


    /**
     * Process messages sent to CC rather than jobExe. Typically,
     * this function works as jobExecutor manager,i.e., release jobExecutor
     * finishing their work and remove it from hashmap
     */
    boolean JobExecutorHandler() {
        boolean retVal = true;
        Message msg = incomingMsgQueue.get();
        if (msg.getMsgType() == Message.MSG_TYPE.jobExecutor_finish) {
            long executorJobid = Long.valueOf(msg.getCmd());
            Message message = new Message(Message.MSG_TYPE.release_jobExecutor,
                    "_", getIdentifier(), getIdentifier(), executorJobid);
            sendTCPMessage(message);
            try {
                jobExecutorMap.get(executorJobid).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            jobInboundMessageMap.remove(executorJobid);
            jobExecutorMap.remove(executorJobid);
        }
        if (jobExecutorMap.size() == 0) {
            retVal = false;
        }
        return retVal;
    }

    private void stopTCPService() {
        tcpService.close();
    }

    private void stopRPCService() {
        if(rpcServer !=null) {
            rpcServer.close();
        }
    }
}

