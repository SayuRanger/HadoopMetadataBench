package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.json.JSONObject;

public class DatanodeStorageParam {
    private String storageUuid;
    private DatanodeStorage.State state;
    private StorageType storageType;

    public static DatanodeStorageParam parseJson(JSONObject object) {
        DatanodeStorageParam param = new DatanodeStorageParam();
        param.setState(UtilJson.getDatanodeStorageState(object, "state"));
        param.setStorageType(UtilJson.getStorageType(object, "storageType"));
        param.setStorageUuid(UtilJson.getString(object, "storageUuid"));
        return param;
    }

    public String getStorageUuid() {
        return storageUuid;
    }

    public void setStorageUuid(String storageUuid) {
        this.storageUuid = storageUuid;
    }

    public DatanodeStorage.State getState() {
        return state;
    }

    public void setState(DatanodeStorage.State state) {
        this.state = state;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }
}
