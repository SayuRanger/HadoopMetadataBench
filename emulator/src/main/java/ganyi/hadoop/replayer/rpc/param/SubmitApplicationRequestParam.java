package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.json.JSONObject;

public class SubmitApplicationRequestParam {
    public static SubmitApplicationRequest parseParam(JSONObject object) {
        ApplicationSubmissionContext context = UtilJson.getApplicationSubmissionContext(object, "application_submission_context");
        return SubmitApplicationRequest.newInstance(context);
        /*        Records.newRecord(SubmitApplicationRequest.class);
        request.setApplicationSubmissionContext(context);*/

    }
}
