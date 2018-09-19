package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class getBlockLocationsSourceParam {
    private String src;
    private long offset;
    private long length;

    public static getBlockLocationsSourceParam parseJson(JSONObject object) {
        getBlockLocationsSourceParam param = new getBlockLocationsSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setOffset(UtilJson.getLong(object, "offset"));
        param.setLength(UtilJson.getLong(object, "length"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
