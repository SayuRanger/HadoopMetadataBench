package ganyi.hadoop.replayer.rpc.param;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.json.JSONObject;

public class StorageBlockReportParam {
    private DatanodeStorage storage;
    private BlockListAsLongs blocks;

    public static StorageBlockReportParam parseJson(JSONObject object) {
        StorageBlockReportParam param = new StorageBlockReportParam();
        param.setStorage(UtilJson.getDatanodeStorage(object, "storage"));
        param.setBlocks(BlockListAsLongs.decodeBuffer(
                UtilJson.getInt(object, "numberOfBlocks"),
                ByteString.EMPTY
        ));
        return param;
    }

    public DatanodeStorage getStorage() {
        return storage;
    }

    public void setStorage(DatanodeStorage storage) {
        this.storage = storage;
    }

    public BlockListAsLongs getBlocks() {
        return blocks;
    }

    public void setBlocks(BlockListAsLongs blocks) {
        this.blocks = blocks;
    }
}
