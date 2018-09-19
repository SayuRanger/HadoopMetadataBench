package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.util.Records;
import org.json.JSONObject;

public class GetApplicationReportRequestParam {
    public static GetApplicationReportRequest parseParam(JSONObject object) {
        GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
        request.setApplicationId(UtilJson.getApplicationId(object, "application_id"));
        return request;
    }
}
