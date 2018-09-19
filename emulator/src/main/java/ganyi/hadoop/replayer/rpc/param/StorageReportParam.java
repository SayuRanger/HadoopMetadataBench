package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.json.JSONObject;

public class StorageReportParam {
    private DatanodeStorage storage;
    private boolean failed;
    private long capacity;
    private long dfsUsed;
    private long remaining;
    private long blockPoolUsed;

    public static StorageReportParam parseJson(JSONObject object) {
        StorageReportParam param = new StorageReportParam();
        param.setStorage(UtilJson.getDatanodeStorage(object, "storage"));
        param.setFailed(false);
        param.setCapacity(UtilJson.getLong(object, "capacity"));
        param.setDfsUsed(UtilJson.getLong(object, "dfsUsed"));
        param.setRemaining(UtilJson.getLong(object, "remaining"));
        param.setBlockPoolUsed(UtilJson.getLong(object, "blockPoolUsed"));
        return param;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public void setStorage(DatanodeStorage storage) {
        this.storage = storage;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public void setDfsUsed(long dfsUsed) {
        this.dfsUsed = dfsUsed;
    }

    public long getRemaining() {
        return remaining;
    }

    public void setRemaining(long remaining) {
        this.remaining = remaining;
    }

    public long getBlockPoolUsed() {
        return blockPoolUsed;
    }

    public void setBlockPoolUsed(long blockPoolUsed) {
        this.blockPoolUsed = blockPoolUsed;
    }
}
