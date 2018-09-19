package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class StorageInfoParam {
    private int layoutVersion;
    private int namespaceID;
    private String clusterID;
    private long cTime;

    public static StorageInfoParam parseJson(JSONObject object) {
        StorageInfoParam param = new StorageInfoParam();
        param.setLayoutVersion((int)UtilJson.getLong(object, "layoutVersion"));
        param.setNamespaceID(UtilJson.getInt(object, "namespceID"));
        param.setcTime(UtilJson.getLong(object, "cTime"));
        param.setClusterID(UtilJson.getString(object, "clusterID"));
        return param;
    }

    public int getLayoutVersion() {
        return layoutVersion;
    }

    public void setLayoutVersion(int layoutVersion) {
        this.layoutVersion = layoutVersion;
    }

    public int getNamespaceID() {
        return namespaceID;
    }

    public void setNamespaceID(int namespaceID) {
        this.namespaceID = namespaceID;
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
    }

    public long getcTime() {
        return cTime;
    }

    public void setcTime(long cTime) {
        this.cTime = cTime;
    }
}
