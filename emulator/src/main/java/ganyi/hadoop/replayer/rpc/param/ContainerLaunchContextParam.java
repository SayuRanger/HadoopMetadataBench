package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ContainerLaunchContextParam {
    public static ContainerLaunchContext parseParam(JSONObject object) {
        Map<String, LocalResource> localResources = UtilJson.getLocalResources(object, "localResources");
        Map<String, String> environment = UtilJson.getStringMap(object, "environment",
                "key", "value");
        List<String> commands = UtilJson.getStringList(object, "command");
        Map<String, ByteBuffer> serviceData = null;
        ByteBuffer tokens = UtilJson.getByteBuffer(object, "tokens");
        Map<ApplicationAccessType, String> acls = UtilJson.getAccessControlList(object, "application_ACLs");
        return ContainerLaunchContext.newInstance(localResources, environment, commands,
                serviceData, tokens, acls);
    }
}
