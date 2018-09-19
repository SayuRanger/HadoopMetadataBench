package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.EnumSetWritable;
import org.json.JSONObject;

public class createSourceParam {
    private String src;
    private FsPermission masked;
    private String clientName;
    private boolean createParent;
    private short replication;
    private EnumSetWritable<CreateFlag> createflag;
    private long blockSize;
    private CryptoProtocolVersion[] cryptoProtocolVersion;

    public static createSourceParam parseJson(JSONObject object) {
        createSourceParam param = new createSourceParam();
        param.setSrc(UtilJson.getString(object, "src"));
        param.setMasked(UtilJson.getFsPermission(object, "masked"));
        param.setClientName(UtilJson.getString(object, "clientName"));
        param.setCreateParent(UtilJson.getBoolean(object, "createParent"));
        param.setCreateflag(UtilJson.getCreateFlag(object, "createFlag"));
        param.setReplication(UtilJson.getShort(object, "replication"));
        param.setBlockSize(UtilJson.getLong(object, "blockSize"));
        param.setCryptoProtocolVersion(UtilJson.getcryptoProtocolVersion(object, "cryptoProtocolVersion"));
        return param;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public FsPermission getMasked() {
        return masked;
    }

    public void setMasked(FsPermission masked) {
        this.masked = masked;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public boolean getCreateParent() {
        return createParent;
    }

    public void setCreateParent(boolean createParent) {
        this.createParent = createParent;
    }

    public short getReplication() {
        return replication;
    }

    public void setReplication(short replication) {
        this.replication = replication;
    }

    public EnumSetWritable<CreateFlag> getCreateflag() {
        return createflag;
    }

    public void setCreateflag(EnumSetWritable<CreateFlag> createflag) {
        this.createflag = createflag;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public CryptoProtocolVersion[] getCryptoProtocolVersion() {
        return cryptoProtocolVersion;
    }

    public void setCryptoProtocolVersion(CryptoProtocolVersion[] cryptoProtocolVersion) {
        this.cryptoProtocolVersion = cryptoProtocolVersion;
    }
}
