package ganyi.hadoop.replayer.rpc;

import javafx.util.Pair;
import org.json.JSONObject;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ganyi on 4/15/2017.
 */
public class RpcRecorder {
    private HashMap<String, Pair<Object, Object>> record ;
    //HashMap<RpcPosition,Pair<Type,Type>> objType = null;
    private HashMap<String, JSONObject> sJsonList;
    private HashMap<String, JSONObject> rJsonList;

    public RpcRecorder() {
        record = new HashMap<>();
        sJsonList = new HashMap<>();
        rJsonList = new HashMap<>();
    }

    public void show() {
        synchronized (System.out) {
            Iterator it = sJsonList.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
            }
        }
    }


    public void addReocord(RpcPosition pos, Object request, Object response) {
        record.put(pos.toString(), new Pair<>(request, response));
        //objType.put(pos,new Pair<Type, Type>(request.getClass(),response.getClass()));
    }

    public void addRequestJson(RpcPosition pos, JSONObject json) {
        sJsonList.put(pos.toString(), json);
        //System.out.println("Request Json size: "+ new Instrumentation().getObjectSize());
    }

    public void addResponseJson(RpcPosition pos, JSONObject json) {
        rJsonList.put(pos.toString(), json);
    }

    public Object getRequest(RpcPosition pos) {
        return record.get(pos.toString()).getKey();
    }

    public Object getResponse(RpcPosition pos) {
        return record.get(pos.toString()).getValue();
    }

    public JSONObject getRequestJson(RpcPosition pos) {
        return sJsonList.get(pos.toString());
    }

    public JSONObject getResponseJson(RpcPosition pos) {
        return rJsonList.get(pos.toString());
    }
}
