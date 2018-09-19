package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class getListingSourceParam {
    private String src;
    private byte[] startAfter;      //null
    private boolean needLocation;

    public static getListingSourceParam parseJson(JSONObject object) {
        getListingSourceParam param = new getListingSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setStartAfter(new byte[0]);
        param.setNeedLocation(UtilJson.getBoolean(object, "needLocation"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public byte[] getStartAfter() {
        return startAfter;
    }

    public void setStartAfter(byte[] startAfter) {
        this.startAfter = startAfter;
    }

    public boolean isNeedLocation() {
        return needLocation;
    }

    public void setNeedLocation(boolean needLocation) {
        this.needLocation = needLocation;
    }
}
