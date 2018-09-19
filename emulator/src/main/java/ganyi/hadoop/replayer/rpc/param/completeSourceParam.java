package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.json.JSONObject;

public class completeSourceParam {
    private String src;
    private String clientName;
    private ExtendedBlock last;
    private long fileId;

    public static completeSourceParam parseJson(JSONObject object) {
        completeSourceParam param = new completeSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setClientName(UtilJson.getString(object, "clientName"));
        param.setLast(UtilJson.getExtendedBlock(object, "last"));
        param.setFileId(UtilJson.getLong(object, "fileId"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ExtendedBlock getLast() {
        return last;
    }

    public void setLast(ExtendedBlock last) {
        this.last = last;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }
}
