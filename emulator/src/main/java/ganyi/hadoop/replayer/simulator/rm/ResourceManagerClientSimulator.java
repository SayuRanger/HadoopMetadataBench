package ganyi.hadoop.replayer.simulator.rm;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.netAddress;
import ganyi.hadoop.replayer.rpc.RpcInterpreter;
import ganyi.hadoop.replayer.rpc.RpcPosition;
import ganyi.hadoop.replayer.rpc.param.GetApplicationReportRequestParam;
import ganyi.hadoop.replayer.rpc.param.GetClusterNodesRequestParam;
import ganyi.hadoop.replayer.rpc.param.GetNewApplicationRequestParam;
import ganyi.hadoop.replayer.rpc.param.SubmitApplicationRequestParam;
import ganyi.hadoop.replayer.simulator.Simulator;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;


public class ResourceManagerClientSimulator extends Simulator {
    ApplicationClientProtocol clientRM;

    int app_id;
    long ts;

    public long getTs() {
        return ts;
    }

    public int getApp_id() {
        return app_id;
    }


    public ResourceManagerClientSimulator(String[] args, GlobalConfigure configuration, MessageQueue<Message> inboundQueue, MessageQueue<Message> outBoundQueue) {
        super(args, configuration, inboundQueue, outBoundQueue);
    }

    /*@Override
    void SetupRpcManager() {
        rpcMgr = new RPCManager(this.rmMaster, this);
    }*/

    public ResourceManagerClientSimulator(String[] args) {
        super(new String[]{"rmc.1"});
        configure = new GlobalConfigure(args[0], "rmc.1");
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
        try {
            clientRM = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
        } catch (IOException e) {
            throw new YarnRuntimeException(e);
        }
    }

    @Override
    public void stopSimulatorRPCConnection() {
        RPC.stopProxy(clientRM);
    }

    @Override
    public void periodicalJob(TimerTaskType type, String[] cmd) {

    }

    @Override
    public void init() {
        String id = envs[0];
        setIdentifier(id);
        LOG = LogFactory.getLog(ResourceManagerClientSimulator.class);
        setStartHBCounter(true);
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
                try {
                    metricsReport();
                } catch (RuntimeException e) {
                    LOG.error("Runtime exception happens when collecting metrics.");
                }
                stopTimers();
                stopRPCService();
                break;
            }
        }
        //RMAppClientRPCTest();
    }

    @Override
    public void playbook() throws IOException {
        String[] cmdset;
        while ((cmdset = script.getNext()) != null) {
            String[] param = interpreter.interpret(cmdset, recorder);
            ActionStation(param[0], param[1], param[2], param[3]);
        }
    }

    @Override
    public String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException {
        String jsonResponse;
        RpcPosition position = new RpcPosition(pos);
        if (cmd.equalsIgnoreCase("submitApplication")) {
            SubmitApplicationRequest param = SubmitApplicationRequestParam.parseParam(object);

            long t1 = System.currentTimeMillis();
            SubmitApplicationResponse response = clientRM.submitApplication(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);

            /*LOG.info(simulator.getIdentifier()+"<"+cmd + "> request param: "+new JSONObject(
                    gson.toJson(param)).toString(2) + "; response: "+
                    new JSONObject(jsonResponse).toString(2));*/
        } else if (cmd.equalsIgnoreCase("getApplicationReport")) {
            GetApplicationReportRequest param =
                    GetApplicationReportRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            GetApplicationReportResponse response = clientRM.getApplicationReport(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);
        } else if (cmd.equalsIgnoreCase("getNewApplication")) {
            GetNewApplicationRequest param = GetNewApplicationRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            GetNewApplicationResponse response = clientRM.getNewApplication(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            app_id = response.getApplicationId().getId();
            ts = response.getApplicationId().getClusterTimestamp();

            sendMessage(Message.MSG_TYPE.obtain_appid,
                    String.valueOf(app_id) + ":" + String.valueOf(ts),
                    getIdentifier(), "CC.1");

            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);
        } else if (cmd.equalsIgnoreCase("getClusterNodes")) {
            GetClusterNodesRequest param = GetClusterNodesRequestParam.parseParam(object);
            long t1 = System.currentTimeMillis();
            GetClusterNodesResponse response = clientRM.getClusterNodes(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);
        } else if (cmd.equalsIgnoreCase("getClusterMetrics")) {
            GetClusterMetricsRequest param =
                    Records.newRecord(GetClusterMetricsRequest.class);
            long t1 = System.currentTimeMillis();
            GetClusterMetricsResponse response = clientRM.getClusterMetrics(param);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            //recorder.addReocord(position, param, response);
            //jsonResponse = gson.toJson(response);
        } else {
            throw new RuntimeException(String.format("Cannot find command %s " +
                    "in ApplicationMasterProtocolPB\n"));
        }
        return "";
    }
}
