package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.*;
import org.json.JSONObject;

public class ApplicationSubmissionContextParam {
    public static ApplicationSubmissionContext parseParam(JSONObject object) {
        ApplicationId applicationId = UtilJson.getApplicationId(object, "application_id");
        String applicationName = UtilJson.getString(object, "application_name");
        String queue = UtilJson.getString(object, "queue");
        Priority priority = Priority.newInstance(0);
        ContainerLaunchContext amContainerSpec = UtilJson.getContainerLaunchContext(object, "am_container_spec");
        boolean isUnmanagedAM = false;
        boolean cancelTokensWhenComplete = true;
        //boolean cancelTokensWhenComplete = UtilJson.getBoolean(object,"cancel_tokens_when_complete");
        int maxAppAttempts = 3;
        Resource resource = UtilJson.getResource(object, "resource");
        String applicationType = UtilJson.getString(object, "applicationType");
        return ApplicationSubmissionContext.newInstance(
                applicationId,
                applicationName,
                queue,
                priority,
                amContainerSpec,
                false,
                true,
                maxAppAttempts,
                resource,
                applicationType,
                false,
                (String) null,
                (String) null
        );
        /*return ApplicationSubmissionContext.newInstance(applicationId,applicationName,queue,priority,
                amContainerSpec, isUnmanagedAM, cancelTokensWhenComplete,maxAppAttempts,resource,applicationType);*/
    }
}
