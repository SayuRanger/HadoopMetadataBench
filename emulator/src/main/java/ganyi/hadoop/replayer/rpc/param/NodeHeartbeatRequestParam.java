package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.json.JSONObject;

public class NodeHeartbeatRequestParam {
    public static NodeHeartbeatRequest parseParam(JSONObject object) {
        NodeStatus nodeStatus = UtilJson.getNodeStatus(object, "node_status");
        MasterKey lastKnownContainerTokenMasterKey = UtilJson.getMasterKey(object, "last_known_container_token_master_key");
        MasterKey lastKnownNMTokenMasterKey = UtilJson.getMasterKey(object, "last_known_nm_token_master_key");
        return NodeHeartbeatRequest.newInstance(nodeStatus,
                lastKnownContainerTokenMasterKey, lastKnownNMTokenMasterKey);
    }
}
