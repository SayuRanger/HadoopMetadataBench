package ganyi.hadoop.replayer.simulator.nn;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.netAddress;
import ganyi.hadoop.replayer.rpc.RpcPosition;
import ganyi.hadoop.replayer.rpc.param.blockReceivedAndDeletedParam;
import ganyi.hadoop.replayer.rpc.param.blockReportParam;
import ganyi.hadoop.replayer.rpc.param.registerDatanodeParam;
import ganyi.hadoop.replayer.rpc.param.sendHeartbeatParam;
import ganyi.hadoop.replayer.simulator.Simulator;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Created by ganyi on 4/14/2017.
 */
public class DataNodeSimulator extends Simulator {
    DatanodeProtocolClientSideTranslatorPB datanodeNN;

    private int port;
    private int xferport;
    private int infoport;
    private int ipcport;
    private int infoSecport;

    public DataNodeSimulator(String[] args,
                             GlobalConfigure configure, int port,
                             MessageQueue<Message> in, MessageQueue<Message> out) {
        super(args, configure, in, out);
        setPort(port);
        setupPort();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int p) {
        port = p;
    }

    public int getXferport() {
        return xferport;
    }

    public void setXferport(int xferport) {
        this.xferport = xferport;
    }

    public int getInfoport() {
        return infoport;
    }

    public void setInfoport(int infoport) {
        this.infoport = infoport;
    }

    public int getIpcport() {
        return ipcport;
    }

    public void setIpcport(int ipcport) {
        this.ipcport = ipcport;
    }

    public int getInfoSecport() {
        return infoSecport;
    }

    /*void validateEmulator(String scriptFile) {
        setupRpcConnection();
        SetupRpcManager(scriptFile);
    }*/

    public void setInfoSecport(int infoSecport) {
        this.infoSecport = infoSecport;
    }

    /*@Override
    void SetupRpcManager() {
        rpcMgr = new RPCManager(this.namenode, this);
    }*/

    void setupPort() {
        setXferport(getPort());
        setInfoport(getPort() + 1);
        setIpcport(getPort() + 2);
        setInfoSecport(getPort() + 3);
    }

    @Override
    public void startRPCService() {
        netAddress nnAddr = configure.getNameNodeAddr();
        InetSocketAddress socketAddress = nnAddr.toInetSock();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://" + nnAddr.getIp() + ":" + nnAddr.getPort());
        try {
            datanodeNN = new DatanodeProtocolClientSideTranslatorPB(socketAddress, conf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }

    @Override
    public void stopSimulatorRPCConnection() {
        RPC.stopProxy(datanodeNN);
    }

    public void init() {
        String id = envs[0];
        setIdentifier(id);
        LOG = LogFactory.getLog(DataNodeSimulator.class);
        setStartHBCounter(false);
    }

    public void periodicalJob(TimerTaskType type, String[] cmd) {
        if (type == TimerTaskType.DataNode_HeartBeat) {
            try {
                playOnce(cmd[0]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
                //LOG.info("Release simulator thread: " + getIdentifier());
                try {
                    metricsReport();
                } catch (RuntimeException e) {
                    LOG.error("Runtime exception happens when collecting metrics.");
                }
                stopTimers();
                stopRPCService();
                break;
            } else if (message.getMsgType() == Message.MSG_TYPE.BRD_command) {
                String[] ss = message.getCmd().split("%");
                /*Thread thread = new Thread(new BRDExecutor(ss, this.rpcMgr));*/
                Thread thread = new Thread(
                        () -> {
                            String brdfactor = configure.getSetting("brdfactor");
                            brdfactor = brdfactor != null && !brdfactor.equalsIgnoreCase("n") ? brdfactor : "1";
                            String sleeptime = String.valueOf((long) (Double.valueOf(brdfactor) * Long.valueOf(ss[3])));
                            executeBRD(ss[1], ss[2], sleeptime, ss[4], ss[5]);
                        }
                );
                thread.start();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String clientID = message.getSrc();
                String dn = message.getDest();
                Message msg = new Message(Message.MSG_TYPE.BRD_finish,
                        ss[1] + "#" + clientID, dn, message.getSrc_pool(), message.getJobID());
                sendMessage(msg);
                //System.out.println("Send BRD_finish: "+ss[1]+"#"+clientID +" at " + message.getSrc_pool());
            } else if (message.getMsgType() == Message.MSG_TYPE.start_HB_count) {
                setStartHBCounter(true);
            }
        }
    }

    public void executeBRD(String pos, String rpc, String timediff, String template, String paramList) {
        String jsonStr = interpreter.genActualJson(template, paramList, recorder);
        try {
            ActionStation(pos, rpc, timediff, jsonStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void playbook() throws IOException {
        String[] cmdset;
        script.setTerminatingPoint(3);
        while ((cmdset = script.getNext()) != null) {
            String[] param = interpreter.interpret(cmdset, recorder);
            ActionStation(param[0], param[1], param[2], param[3]);
        }
        //set timer
        String heartbeatinterval = getConfigure()
                .getSetting("heartbeatinterval", "3000");
        //heartbeatinterval = heartbeatinterval!=null?heartbeatinterval:"3000";

        String[] dncmd = new String[]{script.getPos(3)};
        //schedule heartbeat
        scheduleTimerTask(Integer.valueOf(heartbeatinterval),
                Simulator.TimerTaskType.DataNode_HeartBeat,
                dncmd);
    }

    @Override
    public String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException {
        String jsonResponse = "";
        RpcPosition position = new RpcPosition(pos);
        if (cmd.equalsIgnoreCase("registerDatanode")) {
            registerDatanodeParam param = registerDatanodeParam.parseJson(object);
            //long t1 = System.currentTimeMillis();
            DatanodeRegistration ret = datanodeNN.registerDatanode(param.getRegistration());
            //long t2 = System.currentTimeMillis();
            //updateLatency(t1,t2,pos,cmd);
            recorder.addReocord(position, param, ret);
            recorder.addRequestJson(position, object);
            jsonResponse = gson.toJson(ret);

            recorder.addResponseJson(position, new JSONObject(jsonResponse));
        } else if (cmd.equalsIgnoreCase("blockReport")) {
            blockReportParam param = blockReportParam.parseJson(object);
            NamespaceInfo nsInfo =
                    (NamespaceInfo) recorder.getResponse(new RpcPosition(0, 0, 0));
            param.getRegistration().setNamespaceInfo(nsInfo);
            //long t1 = System.currentTimeMillis();
            DatanodeCommand ret =
                    datanodeNN.blockReport(param.getRegistration(),
                            param.getBlockPoolId(), param.getReports(), param.getContext());
            //long t2 = System.currentTimeMillis();
            //updateLatency(t1,t2,pos,cmd);
            recorder.addReocord(position, param, ret);
            if (ret == null) {
                jsonResponse = new JSONObject().toString();
            } else {
                jsonResponse = gson.toJson(ret);
            }
            recorder.addRequestJson(position, object);
            recorder.addResponseJson(position, new JSONObject(jsonResponse));
            //System.out.printf("Response form blockReport: %s\n",jsonResponse);
        } else if (cmd.equalsIgnoreCase("versionRequest")) {
            //long t1 = System.currentTimeMillis();
            NamespaceInfo info = datanodeNN.versionRequest();
            //long t2 = System.currentTimeMillis();
            //updateLatency(t1,t2,pos,cmd);
            recorder.addReocord(position, null, info);

            recorder.addRequestJson(position, object);
            jsonResponse = gson.toJson(info);
            recorder.addResponseJson(position, new JSONObject(jsonResponse));
        } else if (cmd.equalsIgnoreCase("blockReceivedAndDeleted")) {
            blockReceivedAndDeletedParam param = blockReceivedAndDeletedParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            datanodeNN.blockReceivedAndDeleted(param.getRegistration(),
                    param.getBlockPoolId(), param.getBlocks());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            //recorder.addReocord(position,param,null);

            jsonResponse = new JSONObject("{\"blockReceivedAndDeleted\":\"complete\"}").toString();
        } else if (cmd.equalsIgnoreCase("sendHeartbeat")) {
            sendHeartbeatParam param = sendHeartbeatParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            datanodeNN.sendHeartbeat(param.getRegistration(),
                    param.getReports(), param.getCacheCapacity(), param.getCacheUsed(),
                    param.getXmitsInProgress(), param.getXceiverCount(), param.getFailedVolumes(),
                    param.getVolumeFailureSummary());
            long t2 = System.currentTimeMillis();
            if (isStartHBCounter()) {
                updateLatency(t1, t2, pos, cmd);
            }
            jsonResponse = new JSONObject().toString();
            //recorder.addRequestJson(position,object);
        }

        //System.out.printf(">>>add new response: %s\n", new JSONObject(jsonResponse).toString());
        return jsonResponse;
    }
}
