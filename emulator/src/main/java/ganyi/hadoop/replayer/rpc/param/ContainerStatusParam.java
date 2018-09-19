package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.json.JSONObject;

public class ContainerStatusParam {
    public static ContainerStatus parseParam(JSONObject object) {
        ContainerState state = UtilJson.getContainerState(object, "state");
        ContainerId containerId = UtilJson.getContainerId(object, "container_id");
        String diagnostics = UtilJson.getString(object, "diagnostics");
        int existStatus = UtilJson.getInt(object, "exit_status");
        return ContainerStatus.newInstance(containerId, state, diagnostics, existStatus);
    }
}
