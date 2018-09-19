package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class getFileInfoSourceParam {
    private String src;

    public static getFileInfoSourceParam parseJson(JSONObject object) {
        getFileInfoSourceParam param = new getFileInfoSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }
}
