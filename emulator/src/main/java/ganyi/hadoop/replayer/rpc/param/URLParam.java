package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.URL;
import org.json.JSONObject;

public class URLParam {
    public static URL parseParam(JSONObject object) {
        String scheme = UtilJson.getString(object, "scheme");
        String host = UtilJson.getString(object, "host");
        int port = UtilJson.getInt(object, "port");
        String file = UtilJson.getString(object, "file");
        return URL.newInstance(scheme, host, port, file);
    }
}
