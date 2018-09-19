package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.json.JSONObject;

public class ReceivedDeletedBlockInfoParam {
    private Block block;
    private ReceivedDeletedBlockInfo.BlockStatus status;
    private String delHints;

    public static ReceivedDeletedBlockInfoParam parseJson(JSONObject object) {
        ReceivedDeletedBlockInfoParam param = new ReceivedDeletedBlockInfoParam();
        param.setBlock(UtilJson.getBlock(object, "block"));
        param.setStatus(UtilJson.getBlockStatus(object, "status"));
        param.setDelHints(UtilJson.getString(object, "deleteHint"));
        return param;
    }

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    public ReceivedDeletedBlockInfo.BlockStatus getStatus() {
        return status;
    }

    public void setStatus(ReceivedDeletedBlockInfo.BlockStatus status) {
        this.status = status;
    }

    public String getDelHints() {
        return delHints;
    }

    public void setDelHints(String delHints) {
        this.delHints = delHints;
    }
}
