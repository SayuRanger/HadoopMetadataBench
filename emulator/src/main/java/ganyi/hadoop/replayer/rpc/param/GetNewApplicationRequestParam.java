package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.util.Records;
import org.json.JSONObject;

public class GetNewApplicationRequestParam {
    public static GetNewApplicationRequest parseParam(JSONObject object) {
        GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
        return request;
    }
}
