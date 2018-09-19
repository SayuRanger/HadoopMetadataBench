package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.JSONObject;

public class ApplicationAttemptIdParam {
    public static ApplicationAttemptId parseParam(JSONObject object) {
        ApplicationId applicationId = UtilJson.getApplicationId(object, "application_id");
        int attemptId = UtilJson.getInt(object, "attemptId");
        return ApplicationAttemptId.newInstance(applicationId, attemptId);
    }
}
