package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.json.JSONObject;

public class NodeIdParam {
    public static NodeId parseParam(JSONObject object) {
        String host = UtilJson.getString(object, "host");
        int port = UtilJson.getInt(object, "port");
        return NodeId.newInstance(host, port);
    }
}
