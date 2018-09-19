package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.json.JSONObject;

public class DatanodeRegistrationParam {
    private DatanodeID dnId;
    private ExportedBlockKeys keys;
    private StorageInfo storageInfo;
    private String softwareVersion;

    public static DatanodeRegistrationParam parseJson(JSONObject object) {
        DatanodeRegistrationParam param = new DatanodeRegistrationParam();
        param.setDnId(UtilJson.getDatanodeID(object, "datanodeID"));
        param.setKeys(UtilJson.getExportedBlockKeys(object, "keys"));
        param.setStorageInfo(UtilJson.getStorageInfo(object, "storageInfo"));
        param.setSoftwareVersion(UtilJson.getString(object, "softwareVersion"));
        return param;
    }

    public DatanodeID getDnId() {
        return dnId;
    }

    public void setDnId(DatanodeID dnId) {
        this.dnId = dnId;
    }

    public ExportedBlockKeys getKeys() {
        return keys;
    }

    public void setKeys(ExportedBlockKeys keys) {
        this.keys = keys;
    }

    public StorageInfo getStorageInfo() {
        return storageInfo;
    }

    public void setStorageInfo(StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public void setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
    }
}
