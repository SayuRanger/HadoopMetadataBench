package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class renameSourceParam {
    private String src;
    private String dst;

    public static renameSourceParam parseJson(JSONObject object) {
        renameSourceParam param = new renameSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setDst(UtilJson.getString(object, "dst"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }
}
