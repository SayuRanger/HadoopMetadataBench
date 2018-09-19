package ganyi.hadoop.replayer.simulator.rm;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.netAddress;
import ganyi.hadoop.replayer.rpc.RpcPosition;
import ganyi.hadoop.replayer.rpc.param.AllocateRequestParam;
import ganyi.hadoop.replayer.rpc.param.FinishApplicationMasterRequestParam;
import ganyi.hadoop.replayer.rpc.param.RegisterApplicationMasterRequestParam;
import ganyi.hadoop.replayer.simulator.Simulator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sound.midi.SysexMessage;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Executors;


public class ResourceManagerAppMasterSimulator extends Simulator {
    ApplicationMasterProtocol appMasterRM;
    volatile ALLOCATE_TYPE allocateState;
    //volatile ALLOCATE_TYPE allocateState;
    boolean startAllocateCounter = false;
    int totalRequest;
    int alreadyRequest;
    int finished;
    UserGroupInformation ugi;
    long responseID;
    double progress;
    long allocTS;
    int count = 0;

    boolean isRegistered = false;
    //List<String> nmSimID;

    public ResourceManagerAppMasterSimulator(String[] args,
                                             GlobalConfigure configuration,
                                             MessageQueue<Message> inboundQueue,
                                             MessageQueue<Message> outBoundQueue) {
        super(args, configuration, inboundQueue, outBoundQueue);
    }

    public ResourceManagerAppMasterSimulator(String[] args) {
        super(new String[]{"rmc.1"});
        configure = new GlobalConfigure(args[0], "rmc.1");
    }

    public int getTotalRequest() {
        return totalRequest;
    }

    public int getAlreadyRequest() {
        return alreadyRequest;
    }

    public void setAlreadyRequest(int alreadyRequest) {
        this.alreadyRequest = alreadyRequest;
    }

    public long getResponseID() {
        return responseID;
    }

    public void setResponseID(long responseID) {
        this.responseID = responseID;
    }

    /*public void updateProgress(){
        //not sure what does progress mean in RM_APP.
        progress = alreadyRequest / totalRequest;
    }*/

    public double getProgress() {
        return progress;
    }

    public ALLOCATE_TYPE getAllocateState() {
        synchronized (allocateState) {
            return allocateState;
        }
    }

    public void setAllocateState(ALLOCATE_TYPE allocateState) {
        synchronized (allocateState) {
            this.allocateState = allocateState;
        }

    }

    @Override
    public void startRPCService() {
        netAddress rmAddr = configure.getResourceManagerAddr();
        InetSocketAddress socketAddress =
                new InetSocketAddress(rmAddr.getIp(), rmAddr.getPort());
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("yarn.resourcemanager.hostname", socketAddress.getAddress().getHostName());
        conf.set("yarn.resourcemanager.address", socketAddress.getAddress().getHostAddress() + ":" + rmAddr.getPort());
        conf.set("yarn.resourcemanager.scheduler.address", socketAddress.getAddress().getHostAddress() + ":" + (rmAddr.getPort() + 1));


        appMasterRM = ugi.doAs(
                (PrivilegedAction<ApplicationMasterProtocol>) () -> {
                    try {
                        return ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
        if (appMasterRM == null) {
            LOG.error("Failed to create rmMaster rpc proxy.");
            System.exit(5);
        }
        /*rmMaster = ClientRMProxy.createRMProxy(conf,ApplicationMasterProtocol.class);*/
    }

    /*@Override
    void SetupRpcManager() {
        rpcMgr = new RPCManager(this.rmMaster, this);
    }*/

    @Override
    public void stopSimulatorRPCConnection() {
        RPC.stopProxy(appMasterRM);
    }

    @Override
    public void periodicalJob(TimerTaskType type, String[] cmd) {
        LOG.info(getIdentifier() + " allocate state: " + this.allocateState.name() + " progress:" + progress);
        if (type == TimerTaskType.AppMaster_allocate) {
            /*if (progress < 1) {*/

                try {
                    int requested = alreadyRequest;
                    if (getAllocateState() == ALLOCATE_TYPE.normal) {
                        playOnce("0,0,2");
                    } else if (getAllocateState() == ALLOCATE_TYPE.resource_request) {
                        LOG.info("process resource request.");
                        playOnce("0,0,1");
                        setAllocateState(ALLOCATE_TYPE.normal);
                    }
                    if(requested < alreadyRequest){
                        if(alreadyRequest < totalRequest){
                            setAllocateState(ALLOCATE_TYPE.resource_request);
                        }
                        else if(alreadyRequest == totalRequest /* && count ==1 */){
                            setAllocateState(ALLOCATE_TYPE.resource_request);
                        }
                    }
                    /*if (requested != alreadyRequest || alreadyRequest == totalRequest) {
                        //request resource next time.
                        LOG.info("Request resource next time.");
                        setAllocateState(ALLOCATE_TYPE.resource_request);
                    }*/
                } catch (IOException e) {
                    e.printStackTrace();
                }
            /*} else {
                LOG.info(getIdentifier()+ " finishAM.");
                try {
                    playOnce("0,0,3");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //rpcMgr.respondWithFinish("RM_AM|" + getJobID());
            }*/
            responseID += 1;
        }
    }

    @Override
    public void init() {
        String id = envs[0];
        totalRequest = Integer.valueOf(envs[1]);
        LOG = LogFactory.getLog(ResourceManagerAppMasterSimulator.class);
        LOG.info("Total number of request NM: " + totalRequest + ".");
        setIdentifier(id);
        alreadyRequest = 0;
        finished = 0;
        responseID = 0;
        progress = alreadyRequest / totalRequest;


        String[] ss = envs[2].split(":");
        Text kind = new Text(ss[0]);
        Text service = new Text(ss[1]);
        byte[] identifier = DatatypeConverter.parseHexBinary(ss[2]);
        byte[] password = DatatypeConverter.parseHexBinary(ss[3]);

        ugi = UserGroupInformation.createRemoteUser("SPARK_USER");
        Token<AMRMTokenIdentifier> token = new Token<>(identifier, password, kind, service);
        ugi.addToken(token);

        setStartHBCounter(true);
    }

    @Override
    public void run() {
        init();
        String jobFile = "";
        while (true) {
            Message message = inboundQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.command) {
                String[] ss = message.getCmd().split("#");
                if (ss[0].equalsIgnoreCase("start")) {
                    LOG.info("Start TS:"+getJobID()+"#"+System.currentTimeMillis()+"#");
                    jobFile = ss[1];
                    validateEmulator(jobFile);
                    try {
                        play("");
                        //rpcMgr.respondWithFinish(jobFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (message.getMsgType() == Message.MSG_TYPE.release_simulator) {
                LOG.info("Release simulator thread: " + getIdentifier());
                setStartHBCounter(false);
                try {
                    metricsReport();
                } catch (RuntimeException e) {
                    LOG.error("Runtime exception happens when collecting metrics.");
                }
                stopRPCService();
                break;
            } else if (message.getMsgType() == Message.MSG_TYPE.start_HB_count) {
                startAllocateCounter = true;
            } else if (message.getMsgType() == Message.MSG_TYPE.ChangeHBState) {
                LOG.info(getIdentifier()+":Receive ChangeHBState in am.");
                if (message.getCmd().equalsIgnoreCase(ResourceManagerAppMasterSimulator
                        .ALLOCATE_TYPE.resource_request.name())) {
                    setAllocateState(ALLOCATE_TYPE.resource_request);
                    LOG.info("Start allocate:"+getJobID()+":"+System.currentTimeMillis()+":");

                    /*LOG.info("stop AM before allocate.");
                    System.exit(5);*/
                    allocTS = System.currentTimeMillis();
                    String[] amcmd = new String[]{""};
                    scheduleAggresiveTimer(200,3000,
                            TimerTaskType.AppMaster_allocate,amcmd);
                    /*scheduleTimerTask(1000,
                            Simulator.TimerTaskType.AppMaster_allocate, amcmd);*/
                }
            } else if (message.getMsgType() == Message.MSG_TYPE.RMAppMasterStatus) {
                if (message.getCmd().equalsIgnoreCase("progress")) {
                    finished += 1;
                    progress = 1.0 * finished / totalRequest;
                    LOG.info("progress: " + progress);
                    //when progress goes to 100%, stop allocate and call finishApplication.
                    if (progress == 1.0) {
                        LOG.info("Progress reach 100%, stop allocate and send finishAppMaster in "+getIdentifier());
                        stopTimers();

                        //shutdown only once.
                        try {
                            playOnce("0,0,3");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        /*while(true) {
                            try {
                                playOnce("0,0,3");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            if(!isRegistered){
                                break;
                            }
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }*/
                        //Notify CC to change state of all NMs.
                        Message msg = new Message(Message.MSG_TYPE.RMAppMasterStatus,
                                "finish", getIdentifier(), "CC.1", getJobID());
                        msg.setMisc(envs[3]);
                        sendMessage(msg);
                    }
                }
                else if (message.getCmd().equalsIgnoreCase("toReleasePhase")) {
                    Message msg = new Message(Message.MSG_TYPE.finish_response,
                            jobFile,getIdentifier(),"CC.1",getJobID());
                    sendMessage(msg);
                }

            }
        }
        //RMAppMasterRPCTest();
    }

    @Override
    public void playbook() throws IOException {
        String[] cmdset;
        LOG.info(getIdentifier()+":Haha, start run RM APP RPC.");
        script.setTerminatingPoint(1);
        long ts = System.currentTimeMillis();
        while ((cmdset = script.getNext()) != null) {
            String[] param = interpreter.interpret(cmdset, recorder);
            ActionStation(param[0], param[1], param[2], param[3]);
        }
        LOG.info("register latency:"+getJobID()+"#"+String.valueOf(System.currentTimeMillis() - ts));
        setAllocateState(ALLOCATE_TYPE.normal);

        //notify CC that am finishes register.
        Message msg = new Message(Message.MSG_TYPE.RMAppMasterStatus,
                "register", getIdentifier(), "CC.1", getJobID());
        sendMessage(msg);
    }

    @Override
    public String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException {
        String jsonResponse;
        RpcPosition position = new RpcPosition(pos);
        if (cmd.equalsIgnoreCase("registerApplicationMaster")) {
            RegisterApplicationMasterRequest param =
                    RegisterApplicationMasterRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            RegisterApplicationMasterResponse response = appMasterRM.registerApplicationMaster(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);
            isRegistered = true;
            //LOG.info(getIdentifier()+ " "+ cmd +" response: "+jsonResponse);

        } else if (cmd.equalsIgnoreCase("allocate")) {
            AllocateRequest param = AllocateRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            AllocateResponse response = appMasterRM.allocate(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            if(getTotalRequest() == getAlreadyRequest()){
                setAlreadyRequest(getTotalRequest() + 1);
            }

            List<Container> containers = response.getAllocatedContainers();
            if (containers.size() != 0) {
                //int vdrSize = containers.size();
                count ++;
                String  s= getIdentifier()+" get "+ containers.size()+
                        " containers from allocate, already get "+getAlreadyRequest();
                if(getAlreadyRequest() < getTotalRequest()) {
                    Message message = new Message(Message.MSG_TYPE.RMAppMasterStatus,
                            "start_nm_container", getIdentifier(), "CC.1",
                            getJobID());
                    StringJoiner sj = new StringJoiner("|");
                    for (Container container : containers) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(container.getNodeId())
                                .append("#").append(container.getId().toString());
                        sj.add(sb.toString());
                    }
                    message.setMisc(sj.toString());
                    sendMessage(message);

                    int requested = getAlreadyRequest();
                    requested += containers.size();
                    setAlreadyRequest(requested);

                    s += " <<launch containers!>>";
                }
                else{
                    s += " <<discard all over-subscribes.>>";
                }
                LOG.info(s);
                /*System.out.println("vdr should equal requested=" + requested +  ", already=" + getAlreadyRequest() +
                    ", vdrSize=" + vdrSize + ", csz=" + containers.size());*/
            }

            if (getTotalRequest() - getAlreadyRequest() == 0) {
                LOG.info("Allocate latency:"+getJobID()+"#"+String.valueOf(System.currentTimeMillis() - allocTS)+"#");
                LOG.info("End TS:"+getJobID()+"#"+System.currentTimeMillis()+"#");
            }
        } else if (cmd.equalsIgnoreCase("finishApplicationMaster")) {

            LOG.info(getIdentifier() +" invokes finishApplicationMaster.");
            FinishApplicationMasterRequest param =
                    FinishApplicationMasterRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            FinishApplicationMasterResponse response = appMasterRM.finishApplicationMaster(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            if (response.getIsUnregistered()) {
                isRegistered = false;

            }
        } else {
            throw new RuntimeException(String.format("Cannot find command %s " +
                    "in ApplicationMasterProtocolPB\n"));
        }
        return "";
    }

    public enum ALLOCATE_TYPE {
        normal,
        resource_request,
    }


}
