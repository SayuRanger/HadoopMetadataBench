package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.json.JSONObject;

import java.util.List;

public class NodeStatusParam {
    public static NodeStatus parseParam(JSONObject object) {
        NodeId nodeId = UtilJson.getNodeId(object, "node_id");
        int responseId = UtilJson.getInt(object, "response_id");
        List<ContainerStatus> containerStatuses = UtilJson.getContainerStatuses(object, "containersStatuses");
        List<ApplicationId> keepAliveApplications = null;
        NodeHealthStatus nodeHealthStatus = UtilJson.getNodeHealthStatus(object, "nodeHealthStatus");
        return NodeStatus.newInstance(nodeId, responseId, containerStatuses, keepAliveApplications, nodeHealthStatus);
    }
}
