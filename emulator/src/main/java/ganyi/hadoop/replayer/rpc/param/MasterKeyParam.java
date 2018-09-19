package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.util.Records;
import org.json.JSONObject;

import java.nio.ByteBuffer;

public class MasterKeyParam {
    public static MasterKey parseParam(JSONObject object) {
        MasterKey masterKey = Records.newRecord(MasterKey.class);
        int serialNo = UtilJson.getInt(object, "key_id");
        ByteBuffer buffer = UtilJson.getByteBuffer(object, "bytes");
        masterKey.setKeyId(serialNo);
        masterKey.setBytes(buffer);
        return masterKey;
    }
}

