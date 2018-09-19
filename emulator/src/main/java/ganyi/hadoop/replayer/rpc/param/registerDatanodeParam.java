package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.json.JSONObject;

public class registerDatanodeParam {
    private DatanodeRegistration registration;

    public static registerDatanodeParam parseJson(JSONObject object) {
        registerDatanodeParam param = new registerDatanodeParam();
        param.setRegistration(UtilJson.getDatanodeRegistration(object, "registration"));
        return param;
    }

    public DatanodeRegistration getRegistration() {
        return registration;
    }

    public void setRegistration(DatanodeRegistration registration) {
        this.registration = registration;
    }
}
