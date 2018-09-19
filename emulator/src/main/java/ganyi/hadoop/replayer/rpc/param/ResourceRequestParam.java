package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.json.JSONObject;

public class ResourceRequestParam {
    public static ResourceRequest parseParam(JSONObject object) {
        String hostName = UtilJson.getString(object, "resource_name");
        int numContainers = UtilJson.getInt(object, "num_containers");
        Priority priority = UtilJson.getPriority(object, "priority");
        Resource capability = UtilJson.getResource(object, "capability");
        return ResourceRequest.newInstance(priority, hostName, capability, numContainers);
    }
}
