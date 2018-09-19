package ganyi.hadoop.replayer.simulator;


import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.MetricsCollector;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.rpc.RPCExecutor;
import ganyi.hadoop.replayer.rpc.RpcInterpreter;
import ganyi.hadoop.replayer.rpc.RpcRecorder;
import ganyi.hadoop.replayer.rpc.RpcScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.concurrent.*;


/**
 * Created by ganyi on 3/28/2017.
 */
public abstract class Simulator implements Runnable,
        SimulatorExecutor,
        RPCExecutor {
    public Log LOG = LogFactory.getLog(getClass());

    protected String[] envs;
    protected String identifier;

    protected MessageQueue<Message> inboundQueue;
    /*RPCManager rpcMgr;*/
    protected GlobalConfigure configure;
    protected RpcInterpreter interpreter;
    protected RpcRecorder recorder;
    protected RpcScript script;
    protected MetricsCollector metrics;
    MessageQueue<Message> outBoundQueue;
    protected boolean isConnected = false;
    private long jobID = -1;
    private long rand;

    boolean startHBCounter = false;
    /*protected HashMap<String, ScheduledFuture> timerMap;
    protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();*/
    SimTimer simTimer;

    protected Simulator(String[] args) {
        this.setupEnvs(args);
    }

    protected Simulator(String[] args,
                        GlobalConfigure configuration,
                        MessageQueue<Message> inboundQueue,
                        MessageQueue<Message> outBoundQueue) {
        this(args);
        this.configure = configuration;
        this.inboundQueue = inboundQueue;
        this.outBoundQueue = outBoundQueue;
    }

    public long getRand() {
        return rand;
    }

    public void setRand(long rand) {
        this.rand = rand;
    }

    public long getJobID() {
        return jobID;
    }

    public void setJobID(long jobID) {
        this.jobID = jobID;
    }

    /*public RPCManager getRpcMgr() {
        return rpcMgr;
    }*/

    public GlobalConfigure getConfigure() {
        return configure;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getHostName() {
        return configure.getLocalInetAddress().getHostName();
    }

    public String getIPaddr() {
        return configure.getLocalInetAddress().getHostAddress();
    }

    /*public void showInfo(){
        System.out.printf("Identifier: %s, localAddress: %s, port %d\n",identifier,localAddress,port);
    }*/

    public MessageQueue<Message> getInboundQueue() {
        return inboundQueue;
    }


    public void scheduleTimerTask(long period, TimerTaskType type, String[] cmd) {
        Runnable task = ()-> periodicalJob(type,cmd);
        simTimer = new SimTimer();
        simTimer.schedule(period,task);
    }

    public void scheduleAggresiveTimer(long initial, long maxInterval, TimerTaskType type, String[] cmd){
        Runnable task = ()->periodicalJob(type,cmd);
        simTimer = new SimTimer();
        simTimer.aggresiveSchedule(initial,maxInterval,task);
    }

    public void scheduleOnce(long period, TimerTaskType type, String[] cmd){
        Runnable task = ()-> periodicalJob(type,cmd);
        SimTimer simTimer = new SimTimer();
        simTimer.scheduleOnce(period,task);
    }

    /*public void scheduleTimerTask(long delay, TimerTaskType type, String[] cmd) {
        Runnable task = ()-> periodicalJob(type,cmd);
        ScheduledFuture<?> future = executorService.schedule(task,delay,TimeUnit.MILLISECONDS);
        timerMap.put(taskName+"|"+getIdentifier(),future);
    }*/

    protected void stopTimers() {
        if(simTimer!=null&&simTimer.isRunning){
            simTimer.cancel();
        }
    }

    public void sendMessage(Message.MSG_TYPE type, String cmd, String src, String dest) {
        Message message = new Message(type, cmd, src, dest, jobID);
        outBoundQueue.put(message);
    }

    public void sendMessage(Message msg) {
        outBoundQueue.put(msg);
    }

    protected final void validateEmulator(String scriptFile) {
        if (!isConnected) {
            startRPCService();
            SetupRpcManager();
            loadScript(scriptFile);
        }
    }

    public void SetupRpcManager() {
        recorder = new RpcRecorder();
        interpreter = new RpcInterpreter(this);
        script = new RpcScript();
        metrics = new MetricsCollector(configure);
    }

    protected final void stopRPCService() {
        isConnected = false;
        stopSimulatorRPCConnection();
    }

    public void setupEnvs(String[] args) {
        envs = args;
    }

    public abstract void init();

    @Override
    public abstract void run();

    public void testRPC(String[] cmds) {
        String[] param = interpreter.interpret(cmds, recorder);
        try {
            //System.out.println(param[3]);
            ActionStation(param[0], param[1], param[2], param[3]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void playOnce(String pos) throws IOException {
        //special for datanode heartbeat
        String[] cmds = script.getNextByPos(pos);
        String[] param = interpreter.interpret(cmds, recorder);
        ActionStation(param[0], param[1], param[2], param[3]);
    }

    public void play(String range) throws IOException {
        if (range.equalsIgnoreCase("")) {
            script.setCursorRange("", "");
        } else if (range.contains("@")) {
            //split
            String[] pos = range.split("@");
            script.setCursorRange(pos[0], pos[1]);
//            LOG.info("setTerminate: "+script.getTerminatingPoint()+
//                    " Cursor pos: "+script.getPos(script.getCursor()));
        }
        long t1 = System.currentTimeMillis();
        playbook();
        long t2 = System.currentTimeMillis();
        metrics.updateInterval(t1, t2);
    }

    public void respondWithFinish(String content) {
        //tell CC that simulator finish.
        sendMessage(Message.MSG_TYPE.finish_response,
                content, getIdentifier(), "CC.1");
    }

    public void updateLatency(long t1, long t2, String pos, String cmd) {
        if(startHBCounter) {
            metrics.updateRpc(cmd, pos, t2 - t1);
        }
    }

    public void setStartHBCounter(boolean startHBCounter) {
        this.startHBCounter = startHBCounter;
    }

    public boolean isStartHBCounter() {
        return startHBCounter;
    }

    protected void doSleep(String timediff) {
        try {
            int sleepTime = Integer.valueOf(!timediff.equals("") ? timediff : "0");
            String throttle = getConfigure().getSetting("throttle");
            if (throttle != null && !throttle.equalsIgnoreCase("n")) {
                int i = Integer.valueOf(throttle);
                sleepTime = sleepTime > i ? i : sleepTime;
            }
            //sleepTime = sleepTime<1000?1000:sleepTime;
            Thread.sleep(sleepTime);
            //Thread.sleep(2000);
        } catch (InterruptedException e) {
            LOG.info("sleep is interrupted.");
        }
    }

    protected JSONObject toJSON(String jsonString) {
        JSONObject object = new JSONObject();
        if (!jsonString.equals("")) {
            object = new JSONObject(jsonString);
        }
        return object;
    }

    public void metricsReport() {
        metrics.report(this);
    }

    public void loadScript(String file) {
        script.loadScript(file);
    }

    public void ActionStation(String pos, String rpc, String timediff, String jsonString) throws IOException {
        doSleep(timediff);
        try {
            ExecuteRPC(pos, rpc.split("\\.")[1], toJSON(jsonString));
        } catch (YarnException e) {
            e.printStackTrace();
        }
    }

    public enum TimerTaskType {
        None,
        DataNode_HeartBeat,
        APPMaster_RenewLease,
        NodeManager_HeartBeat,
        AppMaster_allocate,
        Container_job_finish,
    }
}

