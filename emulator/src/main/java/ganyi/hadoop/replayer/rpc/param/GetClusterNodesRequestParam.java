package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.util.Records;
import org.json.JSONObject;

import java.util.EnumSet;


public class GetClusterNodesRequestParam {
    public static GetClusterNodesRequest parseParam(JSONObject object) {
        GetClusterNodesRequest request = Records.newRecord(GetClusterNodesRequest.class);
        EnumSet<NodeState> set = EnumSet.noneOf(NodeState.class);
        set.add(NodeState.RUNNING);
        request.setNodeStates(set);
        return request;
    }
}
