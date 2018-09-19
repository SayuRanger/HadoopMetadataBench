package ganyi.hadoop.replayer.controller.common;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class JobScheduler {
    public static Log LOG = LogFactory.getLog(JobScheduler.class);

    Queue<String> jobList;

    GlobalConfigure configure;
    CentralController parent;
    int dataNodeNum;
    int nodeManagerNum;
    long jobID;
    List<String> simPoolList;
    int simPoolIndex;
    List<String> initSimulatorList;

    public JobScheduler(String jobListFile, CentralController parent) {
        this.parent = parent;
        configure = parent.configure;
        jobID = parent.jobID;
        simPoolIndex = -1;
        simPoolList = configure.getSimulatorPoolSet();
        jobList = new LinkedList<>();
        dataNodeNum = 0;
        nodeManagerNum = 0;
        parseAndLoadJob(jobListFile);

    }

    public List<String> getInitSimulatorList() {
        return initSimulatorList;
    }

    String getNextSimpoolID() {
        simPoolIndex += 1;
        if (simPoolIndex >= simPoolList.size() || simPoolIndex < 0) {
            simPoolIndex = 0;
        }
        return simPoolList.get(simPoolIndex);
    }

    void parseAndLoadJob(String jobListFile) {
        try {
            FileReader file = new FileReader(jobListFile);
            BufferedReader bufferedReader = new BufferedReader(file);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                String[] ss = line.split(" ");
                if (ss.length != 4) {
                    throw new RuntimeException("Job format<" + line + "> is incorrect.");
                }
                String s = ss[1] + "#" + ss[2] + "#" + ss[3];
                jobList.add(s);
                String folder = ss[2];
                File dir = new File(folder);
                File[] fileList = dir.listFiles();
                for (File f : fileList) {
                    String[] fss = f.getName().split("\\.");
                    if (fss[0].toLowerCase().contains("dn")) {
                        dataNodeNum += 1;
                    }
                    if (fss[0].toLowerCase().contains("nm")) {
                        nodeManagerNum += 1;
                    }
                }
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getTotaljob(){
        return jobList.size();
    }

    public String getNextJob() {
        String str;
        return (str = jobList.poll()) == null ? "" : str;

    }

    public String getIdentifier() {
        return parent.getIdentifier();
    }

    void setupNodeManager() {
        LOG.info("start all NodeManager.");
        final String nmPrefix = "nm.";
        initSimulatorList = new ArrayList<>();
        String nmperround = configure.getSetting("nmperround", "2000");
        int factor = Integer.valueOf(nmperround);
        nodeManagerNum = nodeManagerNum + nodeManagerNum/10;
        int remainder = nodeManagerNum % factor;
        int round = nodeManagerNum / factor;

        for (int i = 0; i < round; i++) {
            for (int j = 0; j < factor; j++) {
                String simPoolID = getNextSimpoolID();
                String simID = nmPrefix + String.valueOf(i * factor + j + 1);
                configure.updateSimulatorMap(simID, simPoolID);
                initSimulatorList.add(simID);

                Message msg = new Message(Message.MSG_TYPE.create_simulator,
                        simID, getIdentifier(),
                        simPoolID, jobID, Message.TaskType.ResourceManager);
                parent.sendTCPMessage(msg);
            }

            for (int j = 0; j < factor; j++) {
                String simID = initSimulatorList.get(i * factor + j);
                Message msg = new Message(Message.MSG_TYPE.command,
                        "start#" + configure.getSetting("nmfile"),
                        getIdentifier(), simID, jobID);
                parent.sendTCPMessage(msg);
            }

            int num = 0;
            MessageQueue<Message> messageQueue = parent.getIncomingMsgQueue();
            while (num < factor) {
                Message message = messageQueue.get();
                if (message.getMsgType() == Message.MSG_TYPE.finish_response
                        && message.getSrc().toLowerCase().contains("nm")) {
                    num += 1;
                    LOG.info("NodeManager finish response from " + message.getSrc());
                } else if (message.getMsgType() == Message.MSG_TYPE.create_simulator) {
                    String id = message.getCmd();
                    String simPoolID = message.getSrc();
                    configure.updateSimulatorMap(id, simPoolID);

                    //update nm mapping: String(host) --> id
                    parent.setNodeManagerMapping(message.getMisc(),id);
                    LOG.info("NodeManager create_finish from " + message.getSrc());
                } else {
                    LOG.info("Other messages: " + message);
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Round " + i);
        }

        for (int j = 0; j < remainder; j++) {
            String simPoolID = getNextSimpoolID();
            String simID = nmPrefix + String.valueOf(round * factor + j + 1);
            configure.updateSimulatorMap(simID, simPoolID);
            initSimulatorList.add(simID);

            Message msg = new Message(Message.MSG_TYPE.create_simulator,
                    simID, getIdentifier(), simPoolID, jobID,
                    Message.TaskType.ResourceManager);
            parent.sendTCPMessage(msg);
        }

        for (int j = 0; j < remainder; j++) {
            String simID = initSimulatorList.get(round * factor + j);
            Message msg = new Message(Message.MSG_TYPE.command,
                    "start#" + configure.getSetting("nmfile"),
                    getIdentifier(), simID, jobID);
            parent.sendTCPMessage(msg);
        }

        int num = 0;
        MessageQueue<Message> messageQueue = parent.getIncomingMsgQueue();
        LOG.info("ready to receive msg.");
        while (num < remainder) {
            //LOG.info("in while loop.");
            Message message = messageQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.finish_response
                    && message.getSrc().toLowerCase().contains("nm")) {
                num += 1;
            } else if (message.getMsgType() == Message.MSG_TYPE.create_simulator) {
                String id = message.getCmd();
                String simPoolID = message.getSrc();
                configure.updateSimulatorMap(id, simPoolID);
                parent.setNodeManagerMapping(message.getMisc(),id);
                LOG.info("NodeManager create_finish from " + message.getSrc());
            } else {
                LOG.info("Other messages: " + message);
            }
        }

        for (String simID : initSimulatorList) {
            Message msg = new Message(Message.MSG_TYPE.start_HB_count, "_",
                    getIdentifier(), simID, jobID);
            parent.sendTCPMessage(msg);
        }

        String cmd = configure.genSimulatorMapString("nm");
        LOG.info("NM mapping:\n" + cmd);
        for (String dest : simPoolList) {
            Message msg = new Message(Message.MSG_TYPE.sync_nm,
                    cmd, getIdentifier(), dest, jobID);
            parent.sendTCPMessage(msg);
        }
    }

    void setupDataNode() {
        LOG.info("start all datanodes.");
        final String dataNodePrefix = "dn.";
        initSimulatorList = new ArrayList<>();
        String dnperround = configure.getSetting("dnperround");
        int factor = dnperround != null ? Integer.valueOf(dnperround) : 500;
        int remainder = dataNodeNum % factor;
        int round = dataNodeNum / factor;
        //batch start
        for (int i = 0; i < round; i++) {
            for (int j = 0; j < factor; j++) {
                String simPoolID = getNextSimpoolID();
                String simID = dataNodePrefix + String.valueOf(i * factor + j + 1);
                configure.updateSimulatorMap(simID, simPoolID);
                initSimulatorList.add(simID);

                Message msg = new Message(Message.MSG_TYPE.create_simulator,
                        simID, getIdentifier(), simPoolID, jobID, Message.TaskType.NameNode);
                parent.sendTCPMessage(msg);
            }

            for (int j = 0; j < factor; j++) {
                String simID = initSimulatorList.get(i * factor + j);
                Message msg = new Message(Message.MSG_TYPE.command,
                        "start#" + configure.getSetting("dnfile"),
                        getIdentifier(), simID, jobID);
                parent.sendTCPMessage(msg);
            }

            int num = 0;
            MessageQueue<Message> messageQueue = parent.getIncomingMsgQueue();
            while (num < factor) {
                Message message = messageQueue.get();
                if (message.getMsgType() == Message.MSG_TYPE.finish_response
                        && message.getSrc().toLowerCase().contains("dn")) {
                    num += 1;
                    //LOG.info("DN finish response from " + message.getSrc());
                } else if (message.getMsgType() == Message.MSG_TYPE.create_simulator) {
                    String id = message.getCmd();
                    String simPoolID = message.getSrc();
                    configure.updateSimulatorMap(id, simPoolID);
                    //LOG.info("DN create_finish from " + message.getSrc());
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Round " + i);
        }

        for (int j = 0; j < remainder; j++) {
            String simPoolID = getNextSimpoolID();
            String simID = dataNodePrefix + String.valueOf(round * factor + j + 1);
            configure.updateSimulatorMap(simID, simPoolID);
            initSimulatorList.add(simID);

            Message msg = new Message(Message.MSG_TYPE.create_simulator,
                    simID, getIdentifier(), simPoolID, jobID, Message.TaskType.NameNode);
            parent.sendTCPMessage(msg);
        }

        for (int j = 0; j < remainder; j++) {
            String simID = initSimulatorList.get(round * factor + j);
            Message msg = new Message(Message.MSG_TYPE.command,
                    "start#" + configure.getSetting("dnfile"),
                    getIdentifier(), simID, jobID);
            parent.sendTCPMessage(msg);
        }

        int num = 0;
        MessageQueue<Message> messageQueue = parent.getIncomingMsgQueue();
        while (num < remainder) {
            Message message = messageQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.finish_response
                    && message.getSrc().toLowerCase().contains("dn")) {
                num += 1;
            } else if (message.getMsgType() == Message.MSG_TYPE.create_simulator) {
                String id = message.getCmd();
                String simPoolID = message.getSrc();
                configure.updateSimulatorMap(id, simPoolID);
            }
        }

        for (String simID : initSimulatorList) {
            Message message = new Message(Message.MSG_TYPE.start_HB_count, "_",
                    getIdentifier(), simID, jobID);
            parent.sendTCPMessage(message);
        }

        String cmd = configure.genSimulatorMapString("dn");
        //System.out.println("DN Mapping message size: "+cmd.getBytes().length);
        //broadcast dn location.
        LOG.info("DN mapping:\n" + cmd);
        for (String dest : simPoolList) {
            Message msg = new Message(Message.MSG_TYPE.sync_dn,
                    cmd, getIdentifier(), dest, jobID);
            parent.sendTCPMessage(msg);
        }
    }
}
