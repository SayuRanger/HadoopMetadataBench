package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.json.JSONObject;

public class addBlockSourceParam {
    private String clientName;
    private ExtendedBlock previous;
    private String src;
    private DatanodeInfo[] excludeNodes;    //null
    private long fileId;
    private String[] favoredNodes;

    public static addBlockSourceParam parseJson(JSONObject object) {
        addBlockSourceParam param = new addBlockSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setClientName(UtilJson.getString(object, "clientName"));
        param.setPrevious(UtilJson.getExtendedBlock(object, "previous"));
        param.setExcludeNodes(null) /*UtilJson.getDatanodeInfo(object,"excludeNodes")*/;
        param.setFileId(UtilJson.getLong(object, "fileId"));
        param.setFavoredNodes(null);
        return param;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ExtendedBlock getPrevious() {
        return previous;
    }

    public void setPrevious(ExtendedBlock previous) {
        this.previous = previous;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public DatanodeInfo[] getExcludeNodes() {
        return excludeNodes;
    }

    public void setExcludeNodes(DatanodeInfo[] excludeNodes) {
        this.excludeNodes = excludeNodes;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public String[] getFavoredNodes() {
        return favoredNodes;
    }

    public void setFavoredNodes(String[] favoredNodes) {
        this.favoredNodes = favoredNodes;
    }
}
