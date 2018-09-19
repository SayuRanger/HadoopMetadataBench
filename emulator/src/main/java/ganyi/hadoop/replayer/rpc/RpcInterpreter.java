package ganyi.hadoop.replayer.rpc;

import ganyi.hadoop.replayer.simulator.Simulator;
import ganyi.hadoop.replayer.simulator.nn.DataNodeSimulator;
import ganyi.hadoop.replayer.simulator.rm.NodeManagerSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerAppMasterSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerClientSimulator;

import javafx.util.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by ganyi on 4/15/2017.
 */
public class RpcInterpreter {

    //RpcRecorder recorder = null;
    Simulator simulator;
    //String rpcPattern;
    //String rpcParamList;

    public RpcInterpreter(Simulator simulator) {
        this.simulator = simulator;
    }

    public static String[] parseCSVLine(int type, String line) {
        String ll = line;
        if (type == 1) {
            return new String[]{"0,0,0",
                    "ApplicationClientProtocolPB.getClusterNodes",
                    "0",
                    "{\"nodeStates\": [\"#0\"]}",
                    "{\"#0\": [\"Y\", \"NS_RUNNING\"]}"};
        }
        return new String[]{null, ll};
    }

    public static String[] parseCSVLine(String s) {
        int state = 0;
        String[] strings = new String[5];
        int depth = 0;
        String tmp = "";
        int index = 0;
        boolean inClause = false;
        for (char c : s.toCharArray()) {
            switch (state) {
                case 0:
                    if (c == '"') {
                        inClause = true;
                        state = 1;
                    } else if (c == ',') {
                        strings[index++] = tmp;
                        tmp = "";
                        state = 0;
                    } else {
                        tmp += c;
                        state = 2;
                    }
                    break;
                case 1:
                    if (c == '"' && depth == 0 && inClause) {
                        inClause = false;
                    } else if (c == '"') {
                        tmp += c;
                        state = 3;
                    } else if (c == ',' && !inClause) {
                        strings[index++] = tmp;
                        tmp = "";
                        state = 0;
                    } else if (c == '{' || c == '[') {
                        depth += 1;
                        tmp += c;
                    } else if (c == '}' || c == ']') {
                        depth -= 1;
                        tmp += c;
                    } else {
                        tmp += c;
                    }
                    break;
                case 2:
                    if (c == ',') {
                        strings[index++] = tmp;
                        tmp = "";
                        state = 0;
                    } else {
                        tmp += c;
                    }
                    break;
                case 3:
                    state = 1;
                    break;
                default:
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < index; i++) {
                        sb.append(strings[i]).append(",");
                    }
                    sb.append(tmp);
                    throw new RuntimeException("Error: should not go to this branch, " +
                            "string already parsed: " + sb.toString());
            }
        }
        strings[index] = tmp;
        return strings;
    }

    static HashMap<String, Pair<String, String>> parseParamList(JSONObject object) {
        HashMap<String, Pair<String, String>> dict = new HashMap<>();
        for (Iterator it = object.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            JSONArray array = object.getJSONArray(key);
            String type = array.getString(0);
            String rule = String.valueOf(array.get(1));
            dict.put(key, new Pair<String, String>(type, rule));
        }
        return dict;
    }

    public static void main(String[] argv) {
        String str = "\"0,0,2\",ResourceTrackerPB.nodeHeartbeat,0,\"{\"\"node_status\"\": {\"\"nodeHealthStatus\"\": {\"\"health_report\"\": \"\"#3\"\", \"\"is_node_healthy\"\": \"\"#2\"\", \"\"last_health_report_time\"\": \"\"#15\"\"}, \"\"containersStatuses\"\": [{\"\"state\"\": \"\"#11\"\", \"\"diagnostics\"\": \"\"#1\"\", \"\"exit_status\"\": \"\"#14\"\", \"\"container_id\"\": {\"\"id\"\": \"\"#5\"\", \"\"app_attempt_id\"\": {\"\"application_id\"\": {\"\"id\"\": \"\"#7\"\", \"\"cluster_timestamp\"\": \"\"#6\"\"}, \"\"attemptId\"\": \"\"#16\"\"}}}], \"\"node_id\"\": {\"\"port\"\": \"\"#0\"\", \"\"host\"\": \"\"#12\"\"}, \"\"response_id\"\": \"\"#9\"\"}, \"\"last_known_container_token_master_key\"\": {\"\"bytes\"\": \"\"#8\"\", \"\"key_id\"\": \"\"#10\"\"}, \"\"last_known_nm_token_master_key\"\": {\"\"bytes\"\": \"\"#4\"\", \"\"key_id\"\": \"\"#13\"\"}}\",\"{\"\"#14\"\": [\"\"Y\"\", -1000], \"\"#16\"\": [\"\"R\"\", \"\"G-SS,,, run-0001-01.0,0,0,4,,, r,,, ApplicationClientProtocolPB.getApplicationReport,application_report.currentApplicationAttemptId.attemptId\"\"], \"\"#13\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, r,,, ResourceTrackerPB.registerNodeManager,container_token_master_key.key_id\"\"], \"\"#5\"\": [\"\"Y\"\", 1], \"\"#7\"\": [\"\"R\"\", \"\"G-SS,,, run-0001-01.0,0,0,4,,, s,,, ApplicationClientProtocolPB.getApplicationReport,application_id.id\"\"], \"\"#3\"\": [\"\"Y\"\", \"\"\"\"], \"\"#0\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, s,,, ResourceTrackerPB.registerNodeManager,node_id.port\"\"], \"\"#4\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, r,,, ResourceTrackerPB.registerNodeManager,container_token_master_key.bytes\"\"], \"\"#12\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, s,,, ResourceTrackerPB.registerNodeManager,node_id.host\"\"], \"\"#15\"\": [\"\"P\"\", \"\"<REPORT_TS>\"\"], \"\"#1\"\": [\"\"Y\"\", \"\"\"\"], \"\"#10\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, r,,, ResourceTrackerPB.registerNodeManager,container_token_master_key.key_id\"\"], \"\"#2\"\": [\"\"Y\"\", true], \"\"#6\"\": [\"\"P\"\", \"\"<CLS_TS>\"\"], \"\"#8\"\": [\"\"R\"\", \"\"L-SS,,, nm.0,0,0,0,,, r,,, ResourceTrackerPB.registerNodeManager,container_token_master_key.bytes\"\"], \"\"#9\"\": [\"\"P\"\", \"\"<RESPONSE_ID>\"\"], \"\"#11\"\": [\"\"Y\"\", \"\"RUNNING\"\"]}\"\n";
        String[] strs = parseCSVLine(str);
        for (String s : strs) {
            System.out.println(s);
        }
    }

    public String[] interpret(String[] inputs, RpcRecorder recorder) {
        String jsonStr = genActualJson(inputs[3], inputs[4], recorder);
        return new String[]{inputs[0], inputs[1], inputs[2], jsonStr};
        /*if(inputs.length == 5) {
            String jsonStr = genActualJson(inputs[3], inputs[4]);
            return new String[]{inputs[0], inputs[1], inputs[2], jsonStr};
        }
        else{
            String jsonStr = genActualJson("","");
            return new String[]{inputs[0],inputs[1],inputs[2],jsonStr};
        }*/
    }

    public String genActualJson(String template, String paramList, RpcRecorder recorder) {
        String finalJson = "";
        if (template.equalsIgnoreCase("")
                || paramList.equalsIgnoreCase("")) {
            /*System.out.println("Skip getActualJson:");
            System.out.printf("template is %sempty, paramList is %sempty\n",
                    template.equalsIgnoreCase("")?"":"not ",
                    paramList.equalsIgnoreCase("")?"":"not ");*/
            return finalJson;
        }
        //System.out.printf("@Template is %s\n@JsonParam is %s\n",template,paramList);
        //System.out.printf("@Template is %s\n",template);
        //System.out.printf("@JsonParam is %s\n",paramList);
        JSONObject jsonTemplate = new JSONObject(template);
        JSONObject jsonParam = new JSONObject(paramList);
        HashMap<String, Pair<String, String>> dict = parseParamList(jsonParam);

        JSONObject object = null;
        object = parseJson(jsonTemplate, dict, recorder);
        //System.out.println(object.toString(2));
        return object.toString();
    }

    JSONObject parseJson(JSONObject obj,
                         HashMap<String, Pair<String, String>> dict,
                         RpcRecorder recorder) {
        JSONObject ret = new JSONObject();
        for (Iterator it = obj.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            Object subObj = obj.get(key);
            if (subObj instanceof JSONObject) {
                ret.put(key, parseJson((JSONObject) subObj, dict, recorder));
            } else if (subObj instanceof JSONArray) {
                ret.put(key, parseJsonArray((JSONArray) subObj, dict, recorder));

            } else {
                String reference = String.valueOf(subObj);
                ret.put(key, fillParam(reference, dict, recorder));
                //ret.put(key, subObj+"&");
            }
        }
        return ret;
    }

    JSONArray parseJsonArray(JSONArray array,
                             HashMap<String, Pair<String, String>> dict,
                             RpcRecorder recorder) {
        JSONArray ret = new JSONArray();
        for (int i = 0; i < array.length(); i++) {
            Object subObj = array.get(i);
            if (subObj instanceof JSONObject) {
                ret.put(parseJson((JSONObject) subObj, dict, recorder));
            } else if (subObj instanceof JSONArray) {
                ret.put(parseJsonArray((JSONArray) subObj, dict, recorder));
            } else {
                String reference = String.valueOf(subObj);
                ret.put(fillParam(reference, dict, recorder));
            }
        }
        return ret;
    }

    String fillParam(String ref,
                     HashMap<String, Pair<String, String>> dict,
                     RpcRecorder recorder) {
        Pair<String, String> tuple = dict.get(ref);
        String type = tuple.getKey();
        String rule = tuple.getValue();
        String result = "#@$";
        if (type.equals("Y")) {
            result = rule;
        } else if (type.equals("M") || type.equals("P")) {
            //fill up parameter with local variable.
            result = applyMRule(rule);
        } else if (type.equals("R")) {
            result = applyRRule(rule, recorder);
        } else {
            try {
                throw new Exception("Find a type other than \"Y\" \"M\" \"R\" \"P\"");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        assert !result.equalsIgnoreCase("#@$");
        return result;
    }

    public String applyRRule(String rule, RpcRecorder recorder) {
        String ret = "";
        //System.out.println("Apply R rule: "+rule);
        String[] tmp = rule.split(",,,");
        String[] ss = new String[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            ss[i] = tmp[i].trim();
            //System.out.println(ss[i]);
        }
        String locMark = ss[0].split("-")[0];
        String[] location = ss[1].split(",");
        RpcPosition position = new RpcPosition(location[1], location[2], location[3]);
        String role = location[0];
        String type = ss[2];
        String rpcAndAttrChain = ss[3];
        String AttrChain = rpcAndAttrChain.split(",")[1];

        if (locMark.equalsIgnoreCase("L")) {
            if (type.equalsIgnoreCase("s")) {
                JSONObject object = recorder.getRequestJson(position);
                if(object == null){
                    System.out.println("Something goes wrong: no object on position "+position
                    +", please check rule "+rule);
                    recorder.show();
                }
                ret = extractRequestString(object, AttrChain);
            } else { //extract value from response: 'r' type
                JSONObject object = recorder.getResponseJson(position);
                if (object == null) {
                    /*System.out.println("POSITION: " + position);
                    recorder.show();*/
                }
                //recorder.show();
                //simulator.LOG.info("@@@@@position: "+position);
                //System.out.println("GYF: object "+object.toString()+" attrChain "+AttrChain);
                ret = extractResponseString(object, AttrChain);

            }
        } else {//lockMark is "G"  same as "L"
            if (type.equalsIgnoreCase("s")) {
                //recorder.show();
                JSONObject object = recorder.getRequestJson(position);
                ret = extractRequestString(object, AttrChain);
            } else { //extract value from response: 'r' type
                JSONObject object = recorder.getResponseJson(position);
                //recorder.show();
                //simulator.LOG.info("@@@@@position: "+position);
                ret = extractResponseString(object, AttrChain);
            }
        }
        return ret;
    }

    String extractResponseString(JSONObject object, String pattern) {
        String ret = "";

        if (pattern.equalsIgnoreCase("block.b.blockId")) {
            if (object == null || object.getJSONObject("b") == null || object.getJSONObject("b").getJSONObject("block") == null) {
                //System.out.println("VDR blockID error object=" + (object != null ? object.toString() : "null"));
            }
            ret = String.valueOf(object.getJSONObject("b").getJSONObject("block").get("blockId"));
        } else if (pattern.equalsIgnoreCase("block.b.generationStamp")) {
            ret = String.valueOf(object.getJSONObject("b").getJSONObject("block").get("generationStamp"));
        } else if (pattern.equalsIgnoreCase("block.b.poolId")) {
            ret = String.valueOf(object.getJSONObject("b").get("poolId"));
        } else if (pattern.equalsIgnoreCase("fs.fileId")) {
            ret = String.valueOf(object.get("fileId"));
        } else if (pattern.equalsIgnoreCase("locations.blocks[0].b.numBytes")) {
            ret = String.valueOf(object.getJSONArray("blocks").getJSONObject(0)
                    .getJSONObject("b").getJSONObject("block").get("numBytes"));
        } else if (pattern.equalsIgnoreCase("registration.datanodeID.ipAddr")) {
            ret = object.getString("ipAddr");
        } else if (pattern.equalsIgnoreCase("info.blockPoolID")) {
            ret = String.valueOf(object.get("blockPoolID"));
        } else if (pattern.equalsIgnoreCase("info.storageInfo.clusterID")) {
            ret = String.valueOf(object.get("clusterID"));
        } else if (pattern.equalsIgnoreCase("info.storageInfo.namespceID")) {
            ret = String.valueOf(object.get("namespaceID"));
        } else if (pattern.equalsIgnoreCase("fs.fileId")) {
            ret = String.valueOf(object.get("fileId"));
        } else if (pattern.equalsIgnoreCase("application_id.cluster_timestamp")) {
            ret = String.valueOf(object.getJSONObject("proto").getJSONObject("applicationId_")
                    .get("clusterTimestamp_"));
        } else if (pattern.equalsIgnoreCase("container_token_master_key.key_id")) {
            ret = String.valueOf(object.getJSONObject("proto").getJSONObject("containerTokenMasterKey_")
                    .get("keyId_"));
        } else if (pattern.equalsIgnoreCase("container_token_master_key.bytes")) {
            JSONArray array = object.getJSONObject("proto").getJSONObject("containerTokenMasterKey_")
                    .getJSONObject("bytes_").getJSONArray("bytes");
            StringJoiner sj = new StringJoiner("#");
            for (int i = 0; i < array.length(); i++) {
                sj.add(String.valueOf(array.getInt(i)));
            }
            ret = sj.toString();
            //System.out.println("bytes: "+ret);
            //System.out.println("bytes_: "+ org.apache.commons.lang.StringEscapeUtils.escapeJava(ret));
        } else if (pattern.equalsIgnoreCase("application_id.id")) {
            ret = String.valueOf(object.getJSONObject("applicationId")
                    .getJSONObject("proto").getInt("id_"));
        } else {
            System.out.printf("Error: get wrong response parameter: %s\n", pattern);
        }

        return ret;
    }

    String extractRequestString(JSONObject object, String chain) {
        String[] array = chain.split("\\.");
        return getStringRecur(object, array);
    }

    String getStringRecur(JSONObject object, String[] chainArray) {
        if (chainArray.length == 1) {
            try {
                return object.getString(chainArray[0]);
            }
            catch (NullPointerException e){
                System.out.println("Chain array is "+chainArray[0]+"\nJson object is "+object);
            }
            return chainArray[0];
        } else if (chainArray[0].contains("[")) {
            String[] ss = chainArray[0].split("\\[");
            int loc = Integer.valueOf(ss[1].split("]")[0]);
            String key = ss[0];
            JSONArray array = object.getJSONArray(key);
            JSONObject jsonObject = array.getJSONObject(loc);

            String[] newArray = new String[chainArray.length - 1];
            for (int i = 1; i < chainArray.length; i++) {
                newArray[i - 1] = chainArray[i];
            }
            return getStringRecur(jsonObject, newArray);
        } else {
            String[] newArray = new String[chainArray.length - 1];
            for (int i = 1; i < chainArray.length; i++) {
                newArray[i - 1] = chainArray[i];
            }
            //System.out.println("chainArray[0]: "+chainArray[0]);
            if (object.has(chainArray[0])) {
                JSONObject innerScope = object.getJSONObject(chainArray[0]);
                return getStringRecur(innerScope, newArray);
            } else {
                throw new RuntimeException("no value is found by key " + chainArray[0] + " in " + object.toString());
            }
        }
    }

    public String applyMRule(String rule) {
        String strFormat = "";
        List<String> paramTable = new ArrayList<String>();
        String mark = "";
        int state = 0;
        for (char ch : rule.toCharArray()) {
            switch (state) {
                case 0:
                    if (ch == '<') {
                        strFormat += "@";
                        mark = "";
                        state = 1;
                    } else {
                        strFormat += ch;
                    }
                    break;
                case 1:
                    if (ch == '>') {
                        paramTable.add(mark);
                        state = 0;
                    } else {
                        mark += ch;
                    }
                    break;
                default:
                    throw new RuntimeException("Error in function applyMRule: should not execute this line.");
            }
        }
        List<String> actual = specialOps(paramTable);
        String result = "";
        int index = 0;
        for (char ch : strFormat.toCharArray()) {
            if (ch != '@') {
                result += ch;
            } else {
                result += actual.get(index);
                index += 1;
            }
        }
        return result;
    }

    List<String> specialOps(List<String> table){
        List<String> result = new ArrayList<String>();
        for (String str : table) {
            if (str.equalsIgnoreCase("JOBID")) {
                String s = String.valueOf(simulator.getJobID());
                result.add(s);
            } else if (str.equalsIgnoreCase("CLIREDUCER")) {
                result.add(simulator.getIdentifier());
            } else if (str.equalsIgnoreCase("CLIRUN")) {
                result.add(simulator.getIdentifier());
            } else if (str.equalsIgnoreCase("RAND")) {
                result.add(String.valueOf(simulator.getRand()));
            } else if (str.equalsIgnoreCase("d")) {
                result.add(simulator.getIdentifier());
            } else if (str.equalsIgnoreCase("CLIAM")) {
                result.add(simulator.getIdentifier());
            } else if (str.equalsIgnoreCase("WCT1")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("WCT2")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("WCT3")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("AT1")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("AT2")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("AT3")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("BT1")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("BT2")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("BT3")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TGT1")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TGT2")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TGT3")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TST1")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TST2")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("TST3")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("UUID")) {
                String s = UUID.randomUUID().toString();
                result.add(s);
            } else if (str.equalsIgnoreCase("NANOTIME")) {
                String s = String.valueOf(System.nanoTime());
                result.add(s);
            } else if (str.equalsIgnoreCase("HOSTNAME")) {
                if (simulator != null) {
                    result.add(simulator.getHostName());
                } else {
                    result.add("LOCALHOST");
                }
            } else if (str.equalsIgnoreCase("IPADDR")) {
                if (simulator != null) {
                    result.add(simulator.getIPaddr());
                } else {
                    result.add("192.168.1.0");
                }
            }
            //datanode
            else if (str.equalsIgnoreCase("XFERPORT")) {
                DataNodeSimulator dataNodeEmulator = (DataNodeSimulator) simulator;
                String s = String.valueOf(dataNodeEmulator.getXferport());
                //System.out.println("+++++ xferport "+s );
                result.add(s);
            } else if (str.equalsIgnoreCase("INFOPORT")) {
                DataNodeSimulator dataNodeEmulator = (DataNodeSimulator) simulator;
                String s = String.valueOf(dataNodeEmulator.getInfoport());
                result.add(s);
            } else if (str.equalsIgnoreCase("IPCPORT")) {
                DataNodeSimulator dataNodeEmulator = (DataNodeSimulator) simulator;
                String s = String.valueOf(dataNodeEmulator.getIpcport());
                result.add(s);
            } else if (str.equalsIgnoreCase("INFOSECUREPORT")) {
                DataNodeSimulator dataNodeEmulator = (DataNodeSimulator) simulator;
                String s = String.valueOf(dataNodeEmulator.getInfoSecport());
                result.add(s);
            }
            //node manager
            else if (str.equalsIgnoreCase("HTTP_PORT")) {
                String s = String.valueOf(8042);
                result.add(s);
            } else if (str.equalsIgnoreCase("PORT")) {
                NodeManagerSimulator nodeManagerSimulator = (NodeManagerSimulator) simulator;
                String s = String.valueOf(nodeManagerSimulator.getPort());
                result.add(s);
            } else if (str.equalsIgnoreCase("REPORT_TS")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("CLS_TS")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("RESPONSE_ID")) {
                if (simulator instanceof NodeManagerSimulator) {
                    long id = ((NodeManagerSimulator) simulator).getResponseID();
                    String s = String.valueOf(id);
                    result.add(s);
                } else if (simulator instanceof ResourceManagerAppMasterSimulator) {
                    long id = ((ResourceManagerAppMasterSimulator) simulator).getResponseID();
                    String s = String.valueOf(id);
                    result.add(s);
                } else {
                    throw new RuntimeException("no handling method for RESPONSE_ID in class " + simulator.getClass());
                }

            } else if (str.equalsIgnoreCase("SPARK_LIB_TS")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("C1TSJOBSPLITMETAINFO")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("CLIENT_HOST")) {
                String s = simulator.getHostName();
                result.add(s);
            } else if (str.equalsIgnoreCase("CLIENT_TRACK_PORT")) {
                String s = String.valueOf(10001);
                result.add(s);
            } else if (str.equalsIgnoreCase("C1TSJOBJAR")) {
                String s = String.valueOf(System.currentTimeMillis());
                result.add(s);
            } else if (str.equalsIgnoreCase("PROGRESS")) {
                if (simulator instanceof ResourceManagerAppMasterSimulator) {
                    String s = String.valueOf(
                            ((ResourceManagerAppMasterSimulator) simulator).getProgress()
                    );
                    result.add(s);
                } else {
                    throw new RuntimeException("no handling method for PROGRESS in class " + simulator.getClass());
                }

            } else if (str.equalsIgnoreCase("NUM_CONTAINER")) {
                int tta = ((ResourceManagerAppMasterSimulator) simulator).getTotalRequest();
                int ttb = ((ResourceManagerAppMasterSimulator) simulator).getAlreadyRequest();
                String s = String.valueOf(
                        ((ResourceManagerAppMasterSimulator) simulator).getTotalRequest() -
                                ((ResourceManagerAppMasterSimulator) simulator).getAlreadyRequest()
                );
                /*System.out.println("GYF " +simulator.getIdentifier()+
                        ": ask for " + s + ", vdr total=" + tta + ", already=" + ttb);*/
                result.add(s);
            } else if (str.equalsIgnoreCase("RPCPORT")) {
                String s = String.valueOf(8808);
                result.add(s);
            } else if (str.equalsIgnoreCase("CONTAINER_ID")) {
                if (simulator instanceof NodeManagerSimulator) {
                    String s = String.valueOf(((NodeManagerSimulator) simulator).getAppSpec().getContainerId());
                    result.add(s);
                } else {
                    throw new RuntimeException("CONTAINER_ID is unknown for " + simulator.getClass());
                }
            } else if (str.equalsIgnoreCase("AM_CLS_TS")) {
                String s = String.valueOf(((NodeManagerSimulator) simulator).getAppSpec().getApplicationId_ts());
                result.add(s);
            } else if (str.equalsIgnoreCase("AM_APP_ID")) {
                String s = String.valueOf(((NodeManagerSimulator) simulator).getAppSpec().getApplicationId_id());
                result.add(s);
            } else if (str.equalsIgnoreCase("AM_ATTEMPT_ID")) {
                String s = String.valueOf(((NodeManagerSimulator) simulator).getAppSpec().getAttemptID());
                result.add(s);
            } else if(str.equalsIgnoreCase("CLIENT_CLS_TS")) {
                String s = String.valueOf(((ResourceManagerClientSimulator) simulator).getTs());
                result.add(s);
            } else if(str.equalsIgnoreCase("APP_ID_ID")){
                String s = String.valueOf(((ResourceManagerClientSimulator)simulator).getApp_id());
                result.add(s);
            }
            else {
                throw new RuntimeException(String.format("Unknown symbol \"%s\" cannot apply to M rule.\n", str));
            }
        }
        return result;
    }
}
