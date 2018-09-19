package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class DatanodeIDParam {
    private String ipAddr;
    private String hostName;
    private String datanodeUuid;
    private int xferPort;
    private int infoPort;
    private int infoSecurePort;
    private int ipcPort;

    public static DatanodeIDParam parseJson(JSONObject object) {
        DatanodeIDParam param = new DatanodeIDParam();
        param.setIpAddr(UtilJson.getString(object, "ipAddr"));
        param.setHostName(UtilJson.getString(object, "hostName"));
        param.setDatanodeUuid(UtilJson.getString(object, "datanodeUuid"));
        param.setXferPort(UtilJson.getInt(object, "xferPort"));
        param.setInfoPort(UtilJson.getInt(object, "infoPort"));
        param.setInfoSecurePort(UtilJson.getInt(object, "infoSecurePort"));
        param.setIpcPort(UtilJson.getInt(object, "ipcPort"));
        return param;
    }

    public String getIpAddr() {
        return ipAddr;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getDatanodeUuid() {
        return datanodeUuid;
    }

    public void setDatanodeUuid(String datanodeUuid) {
        this.datanodeUuid = datanodeUuid;
    }

    public int getXferPort() {
        return xferPort;
    }

    public void setXferPort(int xferPort) {
        this.xferPort = xferPort;
    }

    public int getInfoPort() {
        return infoPort;
    }

    public void setInfoPort(int infoPort) {
        this.infoPort = infoPort;
    }

    public int getInfoSecurePort() {
        return infoSecurePort;
    }

    public void setInfoSecurePort(int infoSecurePort) {
        this.infoSecurePort = infoSecurePort;
    }

    public int getIpcPort() {
        return ipcPort;
    }

    public void setIpcPort(int ipcPort) {
        this.ipcPort = ipcPort;
    }
}
