package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.json.JSONObject;

public class RegisterApplicationMasterRequestParam {
    public static RegisterApplicationMasterRequest parseParam(JSONObject object) {
        String host = UtilJson.getString(object, "host");
        int port = UtilJson.getInt(object, "rpc_port");
        String trackingUrl = UtilJson.getString(object, "tracking_url");
        return RegisterApplicationMasterRequest.newInstance(host, port, trackingUrl);
    }
}
