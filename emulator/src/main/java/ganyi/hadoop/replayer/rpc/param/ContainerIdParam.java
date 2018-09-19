package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.json.JSONObject;

public class ContainerIdParam {
    public static ContainerId parseParam(JSONObject object) {
        ApplicationAttemptId applicationAttemptId =
                UtilJson.getApplicationAttemptId(object, "app_attempt_id");
        long containerId = UtilJson.getLong(object, "id");
        return ContainerId.newContainerId(applicationAttemptId, containerId);
    }
}
