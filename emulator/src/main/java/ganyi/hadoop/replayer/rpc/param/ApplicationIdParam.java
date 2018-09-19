package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.JSONObject;

public class ApplicationIdParam {
    public static ApplicationId parseParam(JSONObject object) {
        long clusterTimeStamp = UtilJson.getLong(object, "cluster_timestamp");
        int id = UtilJson.getInt(object, "id");
        return ApplicationId.newInstance(clusterTimeStamp, id);
    }
}
