package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.json.JSONObject;

public class NodeHealthStatusParam {
    public static NodeHealthStatus parseParam(JSONObject object) {
        boolean isNodeHealth = UtilJson.getBoolean(object, "is_node_healthy");
        String healthReport = UtilJson.getString(object, "health_report");
        long lastHealthReport = UtilJson.getLong(object, "last_health_report_time");
        return NodeHealthStatus.newInstance(isNodeHealth, healthReport, lastHealthReport);
    }
}
