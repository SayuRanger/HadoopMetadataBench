package ganyi.hadoop.replayer.simulator.rm;

import com.sun.jersey.json.impl.provider.entity.JSONArrayProvider;
import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.netAddress;
import ganyi.hadoop.replayer.rpc.RpcPosition;
import ganyi.hadoop.replayer.rpc.param.NodeHeartbeatRequestParam;
import ganyi.hadoop.replayer.rpc.param.RegisterNodeManagerRequestParam;
import ganyi.hadoop.replayer.simulator.Simulator;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

public class NodeManagerSimulator extends Simulator {
    ResourceTracker trackerNM;
    NodeManagerHB_TYPE HBState;
    NM_TYPE nm_type = NM_TYPE.NULL;
    long responseID;
    long executorTaskID;

    AppSpec appSpec;

    int port;

    public NodeManagerSimulator(String[] args,
                                GlobalConfigure configuration,
                                int port,
                                MessageQueue<Message> inboundQueue,
                                MessageQueue<Message> outBoundQueue) {
        super(args, configuration, inboundQueue, outBoundQueue);
        this.port = port;
    }

    public AppSpec getAppSpec() {
        return appSpec;
    }

    public NodeManagerSimulator(String[] args) {
        super(new String[]{"nmc.1"});
        configure = new GlobalConfigure(args[0], "nmc.1");
    }

    public long getExecutorTaskID() {
        return executorTaskID;
    }

    public void setExecutorTaskID(long executorTaskID) {
        this.executorTaskID = executorTaskID;
    }

    public void fillContainerId(String str) {
        appSpec = new AppSpec(ContainerId.fromString(str));
    }

    public int getPort() {
        return port;
    }

    public NodeManagerHB_TYPE getHBState() {
        synchronized (HBState) {
            return HBState;
        }
    }

    public void setHBState(NodeManagerHB_TYPE HBState) {
        synchronized (HBState) {
            this.HBState = HBState;
        }

    }

    public long getResponseID() {
        return responseID;
    }

    public void setResponseID(long responseID) {
        this.responseID = responseID;
    }

    @Override
    public void startRPCService() {
        netAddress rmAddr = configure.getResourceManagerAddr();
        InetSocketAddress socketAddress = rmAddr.toInetSock();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("yarn.resourcemanager.hostname", socketAddress.getAddress().getHostName());
        conf.set("yarn.resourcemanager.address", socketAddress.getAddress().getHostAddress() + ":" + rmAddr.getPort());
        conf.set("yarn.resourcemanager.scheduler.address", socketAddress.getAddress().getHostAddress() + ":" + (rmAddr.getPort() + 1));
        try {
            trackerNM = ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }

    @Override
    public void stopSimulatorRPCConnection() {
        RPC.stopProxy(trackerNM);
    }

    /*@Override
    void SetupRpcManager() {
        rpcMgr = new RPCManager(this.tracker, this);
    }*/

    @Override
    public void periodicalJob(TimerTaskType type, String[] cmd) {
        if (type == TimerTaskType.NodeManager_HeartBeat) {
            /*LOG.info("HB type: " + getHBState().name() +
                    " nm_type: " + nm_type.name());*/
            try {
                if (getHBState() == NodeManagerHB_TYPE.normal) {
                    playOnce("0,0,1");
                } else if (getHBState() == NodeManagerHB_TYPE.running &&
                        nm_type == NM_TYPE.applicationMaster) {
                    playOnce("0,0,2");
                } else if (getHBState() == NodeManagerHB_TYPE.running &&
                        nm_type == NM_TYPE.other) {
                    playOnce("0,1,2");
                } else if (getHBState() == NodeManagerHB_TYPE.complete &&
                        nm_type == NM_TYPE.applicationMaster) {
                    playOnce("0,0,3");
                    setHBState(NodeManagerHB_TYPE.normal);

                    LOG.info("running task finished in AM NM<" + getIdentifier() + ">.");
                } else if (getHBState() == NodeManagerHB_TYPE.complete &&
                        nm_type == NM_TYPE.other) {
                    playOnce("0,1,3");

                    setHBState(NodeManagerHB_TYPE.normal);

                    LOG.info("running task finished in other " +
                            "NM<" + getIdentifier() + ">.");
                } else {
                    LOG.error("Unknown HBState: " + getHBState().name()+", nm type: "+nm_type + ", id: "+getIdentifier());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            responseID +=1;
        } else if (type == TimerTaskType.Container_job_finish) {
            //LOG.info("Task in container is finished.");
            if (nm_type == NM_TYPE.other) {
                Message message = new Message(
                        Message.MSG_TYPE.nm_message, "finish_container",
                        getIdentifier(), "CC.1", getExecutorTaskID()
                );
                sendMessage(message);
            } else if (nm_type == NM_TYPE.applicationMaster) {
                Message message = new Message(
                        Message.MSG_TYPE.nm_message, "finish_am_container",
                        getIdentifier(), "CC.1", getExecutorTaskID()
                );
                sendMessage(message);
            } else {
                throw new RuntimeException("NM with nm_type <" + nm_type + "> cannot report finish.");
            }
        } else {
            //error
            throw new RuntimeException("Unknown timerTaskType.");
        }
    }

    @Override
    public void init() {
        String id = envs[0];
        //nm_type = NM_TYPE.valueOf(envs[1]);
        setIdentifier(id);
        responseID = 0;
        LOG = LogFactory.getLog(NodeManagerSimulator.class);
    }

    @Override
    public void run() {
        init();
        while (true) {
            Message message = inboundQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.command) {
                String[] ss = message.getCmd().split("#");
                if (ss[0].equalsIgnoreCase("start")) {
                    String jobFile = ss[1];
                    validateEmulator(jobFile);
                    try {
                        play("");
                        respondWithFinish(jobFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (message.getMsgType() == Message.MSG_TYPE.release_simulator) {
                LOG.info("Release simulator thread: " + getIdentifier());
                setStartHBCounter(false);
                stopTimers();

                try {
                    metricsReport();
                } catch (RuntimeException e) {
                    LOG.error("Runtime exception happens when collecting metrics.");
                    e.printStackTrace();
                }

                stopRPCService();
                break;
            } else if (message.getMsgType() == Message.MSG_TYPE.start_HB_count) {
                setStartHBCounter(true);
            } else if (message.getMsgType() == Message.MSG_TYPE.ChangeHBState) {
                /*LOG.info("AM's container: change heart beat state to " + message.getCmd());*/
                NodeManagerHB_TYPE state = NodeManagerHB_TYPE.valueOf(message.getCmd());
                if (state == NodeManagerHB_TYPE.running) {
                    setExecutorTaskID(message.getJobID());
                    nm_type = NM_TYPE.applicationMaster;
                    fillContainerId(message.getMisc().split("\\|")[1]);
                    setHBState(state);

                    Message msg = new Message(Message.MSG_TYPE.nm_message,
                            "amnm_started", getIdentifier(), "CC.1", getExecutorTaskID());
                    msg.setMisc(message.getMisc().split("\\|")[0]);
                    sendMessage(msg);
                } else if (state == NodeManagerHB_TYPE.complete) {
                    LOG.info("Change state of NM to complete at sim "+getIdentifier());
                    setHBState(state);
                }
            } else if (message.getMsgType() == Message.MSG_TYPE.nm_message) {
                if (message.getCmd().equalsIgnoreCase("start_container")) {
                    setExecutorTaskID(message.getJobID());

                    nm_type = NM_TYPE.other;

                    String misc = message.getMisc();
                    fillContainerId(misc.split(":")[1]);
                    long timeMilli = Long.valueOf(misc.split(":")[0]) * 1000;
                    setHBState(NodeManagerHB_TYPE.running);
                    LOG.info("Start running containers on " + getIdentifier() + " for "
                            + timeMilli + " ms.");

                    try {
                        Thread.sleep(timeMilli);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Message msg = new Message(
                            Message.MSG_TYPE.nm_message, "finish_container",
                            getIdentifier(), "CC.1", getExecutorTaskID()
                    );
                    sendMessage(msg);
                    /*scheduleOnce(timeMilli, TimerTaskType.Container_job_finish,
                            new String[]{""});*/
                }
            }
        }
    }

    @Override
    public void playbook() throws IOException {
        String[] cmdset;
        script.setTerminatingPoint(1);
        while ((cmdset = script.getNext()) != null) {
            String[] param = interpreter.interpret(cmdset, recorder);
            ActionStation(param[0], param[1], param[2], param[3]);
        }
        setHBState(NodeManagerSimulator.NodeManagerHB_TYPE.normal);
        String[] nmcmd = new String[]{""/*NodeManagerSimulator.NodeManagerHB_TYPE.normal.name()*/};
        scheduleTimerTask(3000, Simulator.TimerTaskType.NodeManager_HeartBeat, nmcmd);
    }

    @Override
    public String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException {
        String jsonResponse;
        RpcPosition position = new RpcPosition(pos);
        if (cmd.equalsIgnoreCase("registerNodeManager")) {
            RegisterNodeManagerRequest param = RegisterNodeManagerRequestParam.parseParam(object);
            RegisterNodeManagerResponse response;
            long t1 = System.currentTimeMillis();
            response = trackerNM.registerNodeManager(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, response);
            jsonResponse = gson.toJson(response);

            recorder.addRequestJson(position, object);
            try {
                recorder.addResponseJson(position, new JSONObject(jsonResponse));
            } catch (JSONException e) {
                throw new IOException("throw from JSON exception: <" + jsonResponse
                        + ">, and respective jsonRequest is " + cmd + ": <" + object.toString() + ">");
            }

        } else if (cmd.equalsIgnoreCase("nodeHeartbeat")) {
            //TODO we need to record more than one heartbeat in a period of time.
            NodeHeartbeatRequest param = NodeHeartbeatRequestParam.parseParam(object);
            NodeHeartbeatResponse response;
            long t1 = System.currentTimeMillis();
            response = trackerNM.nodeHeartbeat(param);
            long t2 = System.currentTimeMillis();

            updateLatency(t1, t2, pos, cmd);

            //recorder.addReocord(position, param, response);
            jsonResponse = gson.toJson(response);

        } else {
            throw new RuntimeException(String.format("Cannot find command %s " +
                    "in ResourceTrackerPB\n"));
        }


        //LOG.info(cmd + " response: " +jsonResponse);
        return jsonResponse;
    }

    public enum NodeManagerHB_TYPE {
        normal,
        running,
        complete,
    }

    public enum NM_TYPE {
        applicationMaster,
        other,
        NULL,
    }

    public class AppSpec {
        public AppSpec(ContainerId id){
            containerId = id.getContainerId();
            applicationId_id = id.getApplicationAttemptId().getApplicationId().getId();
            applicationId_ts = id.getApplicationAttemptId().getApplicationId().getClusterTimestamp();
            attemptID = id.getApplicationAttemptId().getAttemptId();
        }
        long containerId;
        int applicationId_id;
        long applicationId_ts;
        int attemptID;

        public long getContainerId() {
            return containerId;
        }

        public int getApplicationId_id() {
            return applicationId_id;
        }

        public long getApplicationId_ts() {
            return applicationId_ts;
        }

        public int getAttemptID() {
            return attemptID;
        }
    }
}
