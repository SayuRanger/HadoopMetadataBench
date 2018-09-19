package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.json.JSONObject;

public class StorageReceivedDeletedBlocksParam {
    private DatanodeStorage storage;
    private ReceivedDeletedBlockInfo[] blocks;

    public static StorageReceivedDeletedBlocksParam parseJson(JSONObject object) {
        StorageReceivedDeletedBlocksParam param = new StorageReceivedDeletedBlocksParam();
        param.setStorage(UtilJson.getDatanodeStorage(object, "storage"));
        param.setBlocks(UtilJson.getReceivedDeletedBlockInfo(object, "blocks"));
        return param;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public void setStorage(DatanodeStorage storage) {
        this.storage = storage;
    }

    public ReceivedDeletedBlockInfo[] getBlocks() {
        return blocks;
    }

    public void setBlocks(ReceivedDeletedBlockInfo[] blocks) {
        this.blocks = blocks;
    }
}
