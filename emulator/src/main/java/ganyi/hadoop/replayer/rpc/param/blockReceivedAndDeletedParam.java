package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.json.JSONObject;

public class blockReceivedAndDeletedParam {
    private DatanodeRegistration registration;
    private String blockPoolId;
    private StorageReceivedDeletedBlocks[] blocks;

    public static blockReceivedAndDeletedParam parseJson(JSONObject object) {
        blockReceivedAndDeletedParam param = new blockReceivedAndDeletedParam();
        param.setRegistration(UtilJson.getDatanodeRegistration(object, "registration"));
        param.setBlockPoolId(UtilJson.getString(object, "blockPoolId"));
        param.setBlocks(UtilJson.getStorageReceivedDeletedBlocks(object, "blocks"));
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

    public StorageReceivedDeletedBlocks[] getBlocks() {
        return blocks;
    }

    public void setBlocks(StorageReceivedDeletedBlocks[] blocks) {
        this.blocks = blocks;
    }
}
