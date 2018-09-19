package ganyi.hadoop.replayer.controller.common;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class TaskExecutor implements Runnable {
    public static Log LOG = LogFactory.getLog(TaskExecutor.class);
    protected long jobID;
    protected TaskScheduler scheduler;
    protected CentralController parent;
    protected GlobalConfigure configure;
    protected MessageQueue<Message> pendingMessageQueue;
    protected String scheduleFolder;
    protected List<String> simulatorList;
    protected Message.TaskType taskType;
    String[] envs;
    List<String> simPoolList;
    int simPoolIndex;
    MessageQueue<Message> outboundMessageQueue;
    protected double userDefineParallelFactor;

    public TaskExecutor(String[] envs,
                        MessageQueue<Message> in,
                        MessageQueue<Message> out,
                        CentralController parent) {
        init(envs, in, out, parent);
    }

    protected TaskExecutor() {
    }

    protected String getIdentifier() {
        return parent.getIdentifier();
    }

    public long getJobID() {
        return jobID;
    }

    public void setJobID(long jobID) {
        this.jobID = jobID;
    }

    protected String getNextSimpoolID() {
        simPoolIndex += 1;
        if (simPoolIndex >= simPoolList.size() || simPoolIndex < 0) {
            simPoolIndex = 0;
        }
        return simPoolList.get(simPoolIndex);
    }

    public void init(String[] envs,
                     MessageQueue<Message> in,
                     MessageQueue<Message> out,
                     CentralController parent) {
        this.parent = parent;
        pendingMessageQueue = in;
        outboundMessageQueue = out;
        this.envs = envs;

        jobID = System.currentTimeMillis();
        simulatorList = new ArrayList<>();
        this.configure = parent.configure;
        simPoolList = configure.getSimulatorPoolSet();
        Random random = new Random(jobID);
        simPoolIndex = random.nextInt(simPoolList.size());
        String parallel = configure.getSetting("parallel");
        userDefineParallelFactor = Double.valueOf(parallel != null ? parallel : "1");

        String script = envs[0];
        scheduleFolder = envs[1].endsWith("/") ? envs[1] : envs[1] + "/";
        scheduler = new TaskScheduler(script, scheduleFolder, taskType);
        LOG.info("jobID is " + jobID + ", type is " + taskType);
    }

    @Override
    abstract public void run();

    protected void destroy() {
        //send release_simulator message to all sims belonged to this task.
        for (String simID : simulatorList) {
            Message msg = new Message(Message.MSG_TYPE.release_simulator, simID, getIdentifier(),
                    configure.getSimPoolID(simID), jobID);
            outboundMessageQueue.put(msg);
        }
        int num = 0;
        while(num < simulatorList.size()){
            Message message = pendingMessageQueue.get();
            if(message.getMsgType() == Message.MSG_TYPE.release_simulator){
                num++;
            }
        }
    }

    protected void respondWithFinish() {
        Message message = new Message(Message.MSG_TYPE.jobExecutor_finish,
                String.valueOf(getJobID()),
                getIdentifier(), getIdentifier(), parent.jobID);
        parent.sendTCPMessage(message);
    }

    protected void initMultipleSimulator(String filePrefix, int num) {
        initMultipleSimulator(filePrefix, num, "");
    }

    protected void initMultipleSimulator(String filePrefix, int num, String misc) {
        List<String> list = new ArrayList<>();
        //First, we create simulator thread.
        for (int i = 0; i < num; i++) {
            String simPoolID = getNextSimpoolID();
            String simID = scheduler.findAvailable(filePrefix) + "|" + jobID;

            list.add(simID);
            String cmd = simID;

            configure.updateSimulatorMap(simID, simPoolID);
            simulatorList.add(simID);

            Message msg = new Message(Message.MSG_TYPE.create_simulator,
                    cmd, getIdentifier(), simPoolID, jobID, taskType);
            msg.setMisc(misc);
            parent.sendTCPMessage(msg);
        }
        for (String s : list) {
            String simID = s;
            Message msg = new Message(Message.MSG_TYPE.command,
                    "start#" + scheduleFolder + simID.split("\\|")[0] + ".csv",
                    getIdentifier(), simID, jobID);
            parent.sendTCPMessage(msg);
        }
    }

    protected abstract void executeTuple(TaskScheduler.ExecutionTuple tuple);
}
