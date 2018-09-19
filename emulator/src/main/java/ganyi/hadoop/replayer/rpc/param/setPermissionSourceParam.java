package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.fs.permission.FsPermission;
import org.json.JSONObject;

public class setPermissionSourceParam {
    private String src;
    private FsPermission permission;

    public static setPermissionSourceParam parseJson(JSONObject object) {
        setPermissionSourceParam param = new setPermissionSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setPermission(UtilJson.getFsPermission(object, "permission"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public FsPermission getPermission() {
        return permission;
    }

    public void setPermission(FsPermission permission) {
        this.permission = permission;
    }
}
