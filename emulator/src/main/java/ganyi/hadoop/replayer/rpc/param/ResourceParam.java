package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.Resource;
import org.json.JSONObject;

public class ResourceParam {
    public static Resource parseParam(JSONObject object) {
        int memory = object.getInt("memory");
        int virtualCores = object.getInt("virtual_cores");
        return Resource.newInstance(memory, virtualCores);
    }
}
