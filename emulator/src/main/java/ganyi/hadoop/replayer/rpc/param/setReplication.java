package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class setReplication {
    private String src;
    private short replication;

    public static setReplication parseJson(JSONObject object) {
        setReplication param = new setReplication();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setReplication(UtilJson.getShort(object, "replication"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public short getReplication() {
        return replication;
    }

    public void setReplication(short replication) {
        this.replication = replication;
    }
}
