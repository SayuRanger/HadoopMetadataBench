package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.Priority;
import org.json.JSONObject;

public class PriorityParam {
    public static Priority parseParam(JSONObject object) {
        int priority = object.getInt("priority");
        return Priority.newInstance(priority);
    }
}
