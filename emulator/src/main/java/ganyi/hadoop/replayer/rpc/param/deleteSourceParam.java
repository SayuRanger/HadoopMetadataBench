package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class deleteSourceParam {
    private String src;
    private boolean recursive;

    public static deleteSourceParam parseJson(JSONObject object) {
        deleteSourceParam param = new deleteSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setRecursive(UtilJson.getBoolean(object, "recursive"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }
}
