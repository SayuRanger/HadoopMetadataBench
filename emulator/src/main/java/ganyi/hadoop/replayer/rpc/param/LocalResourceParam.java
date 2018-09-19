package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.json.JSONObject;

public class LocalResourceParam {
    public static LocalResource parseParam(JSONObject object) {
        URL url = UtilJson.getURL(object, "resource");
        LocalResourceType type = UtilJson.getLocalResourceType(object, "type");
        LocalResourceVisibility visibility = UtilJson.getLocalResourceVisibility(object, "visibility");
        long size = UtilJson.getLong(object, "size");
        long timestamp = UtilJson.getLong(object, "timestamp");
        return LocalResource.newInstance(url, type, visibility, size, timestamp);
    }
}
