package ganyi.hadoop.replayer.rpc.param;


import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.json.JSONObject;

public class RegisterNodeManagerRequestParam {
    public static RegisterNodeManagerRequest parseParam(JSONObject object) {
        NodeId nodeId = UtilJson.getNodeId(object, "node_id");
        int httpPort = UtilJson.getInt(object, "http_port");
        Resource resource = UtilJson.getResource(object, "resource");
        String nodeManagerVersionId = UtilJson.getString(object, "nm_version");
        return RegisterNodeManagerRequest.newInstance(nodeId, httpPort, resource, nodeManagerVersionId,
                null, null);
    }
}
