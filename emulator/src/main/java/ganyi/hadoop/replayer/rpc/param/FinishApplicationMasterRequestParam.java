package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.json.JSONObject;

public class FinishApplicationMasterRequestParam {
    public static FinishApplicationMasterRequest parseParam(JSONObject object) {
        FinalApplicationStatus finalAppStatus = UtilJson.getFinalApplicationStatus(object, "final_application_status");
        String diagnostics = UtilJson.getString(object, "diagnostics");
        String url = UtilJson.getString(object, "tracking_url");
        return FinishApplicationMasterRequest.newInstance(finalAppStatus, diagnostics, url);
    }
}
