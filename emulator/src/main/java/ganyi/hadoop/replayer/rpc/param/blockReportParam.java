package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.json.JSONObject;

public class blockReportParam {
    private DatanodeRegistration registration;
    private String blockPoolId;
    private StorageBlockReport[] reports;
    private BlockReportContext context;

    public static blockReportParam parseJson(JSONObject object) {
        blockReportParam param = new blockReportParam();
        param.setRegistration(UtilJson.getDatanodeRegistration(object, "registration"));
        param.setBlockPoolId(UtilJson.getString(object, "blockPoolId"));
        param.setContext(UtilJson.getBlockReportContext(object, "context"));
        param.setReports(UtilJson.getStorageBlockReports(object, "reports"));
        return param;
    }

    public DatanodeRegistration getRegistration() {
        return registration;
    }

    public void setRegistration(DatanodeRegistration registration) {
        this.registration = registration;
    }

    public String getBlockPoolId() {
        return blockPoolId;
    }

    public void setBlockPoolId(String blockPoolId) {
        this.blockPoolId = blockPoolId;
    }

    public StorageBlockReport[] getReports() {
        return reports;
    }

    public void setReports(StorageBlockReport[] reports) {
        this.reports = reports;
    }

    public BlockReportContext getContext() {
        return context;
    }

    public void setContext(BlockReportContext context) {
        this.context = context;
    }
}
