package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.fs.permission.FsPermission;
import org.json.JSONObject;

public class mkdirsSourceParam {
    private String src;
    private boolean createParent;
    private FsPermission masked;

    public static mkdirsSourceParam parseJson(JSONObject object) {
        mkdirsSourceParam param = new mkdirsSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setCreateParent(UtilJson.getBoolean(object, "createParent"));
        param.setMasked(UtilJson.getFsPermission(object, "masked"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public boolean getCreateParent() {
        return createParent;
    }

    public void setCreateParent(boolean createParent) {
        this.createParent = createParent;
    }

    public FsPermission getMasked() {
        return masked;
    }

    public void setMasked(FsPermission masked) {
        this.masked = masked;
    }
}
