package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class fsyncSourceParam {
    private String src;
    private long fileId;
    private String clientName;
    private long lastBlockLength;

    public static fsyncSourceParam parseJson(JSONObject object) {
        fsyncSourceParam param = new fsyncSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setFileId(UtilJson.getLong(object, "fileId"));
        param.setClientName(UtilJson.getString(object, "client"));
        param.setLastBlockLength(UtilJson.getLong(object, "lastBlockLength"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public long getLastBlockLength() {
        return lastBlockLength;
    }

    public void setLastBlockLength(long lastBlockLength) {
        this.lastBlockLength = lastBlockLength;
    }
}
