package ganyi.hadoop.replayer.simulator.nn;

import ganyi.hadoop.replayer.GlobalConfigure;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.network.netAddress;
import ganyi.hadoop.replayer.rpc.RpcPosition;
import ganyi.hadoop.replayer.rpc.param.*;
import ganyi.hadoop.replayer.simulator.Simulator;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by ganyi on 4/5/2017.
 */
public class ClientSimulator extends Simulator {
    ClientProtocol clientNN = null;
    SharedCondCounter BRDCounter;
    CLIENT_TYPE type;
    Queue<String> dnForBRD;

    public ClientSimulator(String[] args,
                           GlobalConfigure configure,
                           MessageQueue<Message> in,
                           MessageQueue<Message> out) {
        super(args, configure, in, out);
    }

    @Override
    public void startRPCService() {
        netAddress nnAddr = configure.getNameNodeAddr();
        InetSocketAddress socketAddress = nnAddr.toInetSock();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://" + nnAddr.getIp() + ":" + nnAddr.getPort());
        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo =
                null;
        try {
            proxyInfo = NameNodeProxies.createProxy(conf, NameNode.getUri(socketAddress), ClientProtocol.class, new AtomicBoolean(false));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.clientNN = proxyInfo.getProxy();
        isConnected = true;
    }

    public SharedCondCounter getBRDCounter() {
        return BRDCounter;
    }

    public void setBRDCounter(SharedCondCounter BRDCounter) {
        this.BRDCounter = BRDCounter;
    }

    public void init() {
        String id = envs[0];
        type = CLIENT_TYPE.valueOf(envs[1]);
        setIdentifier(id);
        LOG = LogFactory.getLog(ClientSimulator.class);
        setStartHBCounter(true);
    }

    @Override
    public void stopSimulatorRPCConnection() {
        RPC.stopProxy(clientNN);
    }

    /*@Override
    void SetupRpcManager() {
        if (rpcMgr == null) {
            rpcMgr = new RPCManager(this.clientNamenode, this, getIdentifier().split("\\.")[0]);

        } else {
            if (rpcMgr.getService() == RPCManager.SERVICE.APPMASTER) {
                //skip setup for application master.
            } else {
                rpcMgr = new RPCManager(this.clientNamenode, this, getIdentifier().split("\\.")[0]);
            }
        }
    }*/

   /* void validateEmulator(String scriptFile) {
        if(!isConnected) {
            setupRpcConnection();
            SetupRpcManager(scriptFile);
            isConnected = true;
        }
    }*/

    @Override
    public void periodicalJob(TimerTaskType type, String[] cmd) {
        if (type == TimerTaskType.APPMaster_RenewLease) {
            try {
                long t1 = System.currentTimeMillis();
                clientNN.renewLease(cmd[0]);
                long t2 = System.currentTimeMillis();
                metrics.updateRenewLease(t2 - t1);
                //LOG.info("renewLease on " + line +". ID: "+getIdentifier());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //@Override
    @Override
    public void run() {
        init();
        while (true) {
            Message message = inboundQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.command) {
                String[] ss = message.getCmd().split("#");
                if (ss[0].equalsIgnoreCase("start")) {
                    //LOG.info("Current stage: "+ message);
                    try {
                        if (ss.length == 2) {
                            String jobFile = ss[1];
                            //setJobID(Long.valueOf(ss[2]));
                            setRand(System.currentTimeMillis());
                            validateEmulator(jobFile);
                            play("");
                            respondWithFinish(jobFile);
                        } else if (ss.length == 3) {
                            String jobFile = ss[1];
                            if (getRand() == 0) {
                                setRand(System.currentTimeMillis());
                            }
                            //setJobID(Long.valueOf(ss[3]));
                            validateEmulator(jobFile);
                            play(ss[2]);
                            respondWithFinish(jobFile + "#" + ss[2]);
                        } else {
                            //error
                            LOG.error("Error: in " + getClass() + "'s func run(), " +
                                    "message <" + message + "> is not valid");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (message.getMsgType() == Message.MSG_TYPE.release_simulator) {
                //LOG.info("Release simulator thread "+ getIdentifier());
                setStartHBCounter(false);
                metricsReport();
                stopTimers();
                stopRPCService();
                break;
            }
        }
    }

    @Override
    public void playbook() throws IOException {
        String[] cmdset;
        switch (type) {
            case appMaster:
                while ((cmdset = script.getNext()) != null) {
                    if (script.getCursor() == 1) {
                        //String dfsName = "DFSClient_NONMAPREDUCE_"+ simulator.getIdentifier()+"_1";
                        String dfsName = "DFSClient_NONMAPREDUCE_" + getRand() + "_1";
                        //Set renewLease
                        String[] strings = new String[]{dfsName};
                        scheduleTimerTask(30 * 1000,
                                Simulator.TimerTaskType.APPMaster_RenewLease,
                                strings);
                    }
                    if (cmdset[1].equalsIgnoreCase("DatanodeProtocol.blockReceivedAndDeleted")) {
                        ResolveBRD(cmdset);
                    } else {
                        checkBRDCount();
                        String[] param = interpreter.interpret(cmdset, recorder);
                        ActionStation(param[0], param[1], param[2], param[3]);

                    }
                }
                break;
            case other:
                while ((cmdset = script.getNext()) != null) {
                    if (cmdset[1].equalsIgnoreCase("DatanodeProtocol.blockReceivedAndDeleted")) {
                        ResolveBRD(cmdset);
                    } else {
                        checkBRDCount();
                        String[] param = interpreter.interpret(cmdset, recorder);
                        ActionStation(param[0], param[1], param[2], param[3]);
                    }
                }
                break;
        }
    }

    private void checkBRDCount() {
        SharedCondCounter brdCounter = getBRDCounter();
        while (brdCounter.get() != 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void ResolveBRD(String[] strings) {
        String pos = strings[0].trim();
        String rpc = strings[1].trim();
        String timediff = strings[2].trim();
        String template = strings[3].trim();
        String valueDic = strings[4].trim();

        JSONObject templateJson = new JSONObject(template);
        JSONObject blockOBJ = templateJson.getJSONArray("blocks")
                .getJSONObject(0).getJSONArray("blocks")
                .getJSONObject(0).getJSONObject("block");

        String[] ss = new String[3];
        ss[0] = blockOBJ.getString("numBytes");
        ss[1] = blockOBJ.getString("genStamp");
        ss[2] = blockOBJ.getString("blockId");

        JSONObject dicJson = new JSONObject(valueDic);

        for (String s : ss) {
            JSONArray array = dicJson.getJSONArray(s);
            String type = array.getString(0);
            String rule = String.valueOf(array.get(1));

            if (type.equalsIgnoreCase("Y")) {
                //array.put(1,tmp);
            } else if (type.equalsIgnoreCase("M") || type.equalsIgnoreCase("P")) {
                //String result =interpreter.applyMRule(rule);
                array.put(0, "Y");
                String result = null;
                result = interpreter.applyMRule(rule);
                array.put(1, result);
            } else if (type.equalsIgnoreCase("R")) {
                array.put(0, "Y");
                //System.out.println("GYF: apply R rule: "+ rule+".");
                String result = interpreter.applyRRule(rule, recorder);

                array.put(1, result);
            }
            dicJson.put(s, array);
        }
        //build msg
        StringBuilder sb = new StringBuilder();
        sb.append("%").append(pos)
                .append("%").append(rpc)
                .append("%").append(timediff)
                .append("%").append(template)
                .append("%").append(dicJson.toString());
        getBRDCounter().inc();
        //System.out.println("inc on " + pos + "#"+simulator.getIdentifier());
        //send to one datanode
        String id = dnForBRD.remove();

        sendMessage(Message.MSG_TYPE.BRD_command, sb.toString(),
                getIdentifier(), id);
        //simulator.sendMessage(sb.toString(),id);
    }

    @Override
    public String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException {
        String jsonResponse = "";
        RpcPosition position = new RpcPosition(pos);
        if (cmd.equalsIgnoreCase("getFileInfo")) {
            //System.out.println("Json object is "+object.toString(2));
            getFileInfoSourceParam param = getFileInfoSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            HdfsFileStatus response = clientNN.getFileInfo(param.getSrc());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            recorder.addReocord(position, param, response);

            if (response != null) {
                jsonResponse = gson.toJson(response);
            } else {
                jsonResponse = "{\"response\":\"void\"}";
            }
        } else if (cmd.equalsIgnoreCase("mkdirs")) {
            mkdirsSourceParam param = mkdirsSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            Boolean ret = clientNN.mkdirs(param.getSrc(), param.getMasked(), param.getCreateParent());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, ret);


            jsonResponse = "{\"response\":\"true\"}";
        } else if (cmd.equalsIgnoreCase("setPermission")) {
            setPermissionSourceParam param = setPermissionSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            clientNN.setPermission(param.getSrc(), param.getPermission());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, null);

            jsonResponse = "{\"response\":\"void\"}";
        } else if (cmd.equalsIgnoreCase("getBlockLocations")) {
            getBlockLocationsSourceParam param = getBlockLocationsSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            LocatedBlocks ret = clientNN.getBlockLocations(param.getSrc(),
                    param.getOffset(), param.getLength());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, ret);

            jsonResponse = gson.toJson(ret);
        } else if (cmd.equalsIgnoreCase("getServerDefaults")) {
            long t1 = System.currentTimeMillis();
            FsServerDefaults ret = clientNN.getServerDefaults();
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            recorder.addReocord(position, null, ret);
            jsonResponse = gson.toJson(ret);
        } else if (cmd.equalsIgnoreCase("create")) {
            createSourceParam param = createSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            HdfsFileStatus ret = clientNN.create(param.getSrc(),
                    param.getMasked(),
                    param.getClientName(),
                    param.getCreateflag(),
                    param.getCreateParent(),
                    param.getReplication(),
                    param.getBlockSize(),
                    param.getCryptoProtocolVersion());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            recorder.addReocord(position, param, ret);

            jsonResponse = gson.toJson(ret);
        } else if (cmd.equalsIgnoreCase("addBlock")) {
            addBlockSourceParam param = addBlockSourceParam.parseJson(object);
            /*try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            long t1 = System.currentTimeMillis();
            LocatedBlock ret = clientNN.addBlock(param.getSrc(), param.getClientName(),
                    param.getPrevious(), null, param.getFileId(),
                    null);
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            recorder.addReocord(position, param, ret);
            jsonResponse = gson.toJson(ret);
            //extract locs from jsonResponse and get DN identifier.
            extractDNid(jsonResponse);

            //System.out.printf("******\naddBlock message return json format:\n%s\n",jsonResponse);
        } else if (cmd.equalsIgnoreCase("complete")) {
            completeSourceParam param = completeSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            Boolean ret = clientNN.complete(param.getSrc(),
                    param.getClientName(),
                    param.getLast(),
                    param.getFileId());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            while (ret != true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                t1 = System.currentTimeMillis();
                ret = clientNN.complete(param.getSrc(),
                        param.getClientName(),
                        param.getLast(),
                        param.getFileId());
                t2 = System.currentTimeMillis();
                updateLatency(t1, t2, pos, cmd);
            }

            recorder.addReocord(position, param, ret);

            jsonResponse = String.format("{\"response\":\"%s\"}", ret.toString());
        } else if (cmd.equalsIgnoreCase("fsync")) {
            fsyncSourceParam param = fsyncSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            clientNN.fsync(param.getSrc(),
                    param.getFileId(), param.getClientName(),
                    param.getLastBlockLength());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, null);

            jsonResponse = "{\"response\":\"void\"}";
        } else if (cmd.equalsIgnoreCase("getListing")) {
            getListingSourceParam param = getListingSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            DirectoryListing ret = clientNN.getListing(param.getSrc(),
                    param.getStartAfter(), param.isNeedLocation());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, ret);

            jsonResponse = gson.toJson(ret);
        } else if (cmd.equalsIgnoreCase("rename")) {
            renameSourceParam param = renameSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            Boolean ret = clientNN.rename(param.getSrc(), param.getDst());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, ret);

            jsonResponse = String.format("{\"response\":\"%s\"}", ret.toString());
        } else if (cmd.equalsIgnoreCase("delete")) {
            deleteSourceParam param = deleteSourceParam.parseJson(object);
            long t1 = System.currentTimeMillis();
            Boolean ret = clientNN.delete(param.getSrc(), param.isRecursive());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);

            recorder.addReocord(position, param, ret);
            jsonResponse = String.format("{\"response\":\"%s\"}", ret.toString());
        } else if (cmd.equalsIgnoreCase("setReplication")) {
            setReplication param = setReplication.parseJson(object);
            long t1 = System.currentTimeMillis();
            Boolean ret = clientNN.setReplication(param.getSrc(), param.getReplication());
            long t2 = System.currentTimeMillis();
            updateLatency(t1, t2, pos, cmd);
            recorder.addReocord(position, param, ret);

            jsonResponse = String.format("{\"response\":\"%s\"}", ret.toString());
        } else {
            throw new RuntimeException("cannot find this cmd: ClientProtocol." + cmd);
        }
        //System.out.println("Record request: position <"+position+ "> object <"+object+">");
        recorder.addRequestJson(position, object);
        try {
            recorder.addResponseJson(position, new JSONObject(jsonResponse));
        } catch (JSONException e) {
            throw new IOException("throw from JSON exception: <" + jsonResponse
                    + ">, and respective jsonRequest is " + cmd + ": <" + object.toString() + ">");
        }

        return jsonResponse;
    }

    void extractDNid(String jsonString) {
        dnForBRD = new LinkedList<String>();
        JSONObject object = new JSONObject(jsonString);
        JSONArray array = object.getJSONArray("locs");
        for (int i = 0; i < array.length(); i++) {
            JSONObject block = array.getJSONObject(i);

            int xferPort = block.getInt("xferPort");
            String identifier = "dn." + (xferPort - 10005) / 1;
            //get id by serach clientPort in global config.
            dnForBRD.add(identifier);
        }
    }

    public enum CLIENT_TYPE {
        appMaster,
        other,
    }
}



