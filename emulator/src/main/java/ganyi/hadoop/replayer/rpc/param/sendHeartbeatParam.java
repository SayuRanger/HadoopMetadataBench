package ganyi.hadoop.replayer.rpc.param;

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.json.JSONObject;

public class sendHeartbeatParam {
    private DatanodeRegistration registration;
    private StorageReport[] reports;
    private long cacheCapacity;
    private long cacheUsed;
    private int xmitsInProgress;
    private int xceiverCount;
    private int failedVolumes;
    private VolumeFailureSummary volumeFailureSummary;

    public static sendHeartbeatParam parseJson(JSONObject object) {
        sendHeartbeatParam param = new sendHeartbeatParam();
        param.setRegistration(UtilJson.getDatanodeRegistration(object, "registration"));
        param.setXceiverCount(UtilJson.getInt(object, "xceiverCount"));
        param.setFailedVolumes(UtilJson.getInt(object, "failedVolumes"));
        param.setXmitsInProgress(UtilJson.getInt(object, "xmitsInProgress"));
        param.setCacheCapacity(UtilJson.getLong(object, "cacheCapacity"));
        param.setCacheUsed(UtilJson.getLong(object, "cacheUsed"));
        param.setReports(UtilJson.getStorageReportList(object, "reports"));
        String[] ss = {"VolumeFailureSummary"};
        param.setVolumeFailureSummary(new VolumeFailureSummary(ss, 10241024, 0));
        return param;
    }

    public DatanodeRegistration getRegistration() {
        return registration;
    }

    public void setRegistration(DatanodeRegistration registration) {
        this.registration = registration;
    }

    public StorageReport[] getReports() {
        return reports;
    }

    public void setReports(StorageReport[] reports) {
        this.reports = reports;
    }

    public long getCacheCapacity() {
        return cacheCapacity;
    }

    public void setCacheCapacity(long cacheCapacity) {
        this.cacheCapacity = cacheCapacity;
    }

    public long getCacheUsed() {
        return cacheUsed;
    }

    public void setCacheUsed(long cacheUsed) {
        this.cacheUsed = cacheUsed;
    }

    public int getXmitsInProgress() {
        return xmitsInProgress;
    }

    public void setXmitsInProgress(int xmitsInProgress) {
        this.xmitsInProgress = xmitsInProgress;
    }

    public int getXceiverCount() {
        return xceiverCount;
    }

    public void setXceiverCount(int xceiverCount) {
        this.xceiverCount = xceiverCount;
    }

    public int getFailedVolumes() {
        return failedVolumes;
    }

    public void setFailedVolumes(int failedVolumes) {
        this.failedVolumes = failedVolumes;
    }

    public VolumeFailureSummary getVolumeFailureSummary() {
        return volumeFailureSummary;
    }

    public void setVolumeFailureSummary(VolumeFailureSummary volumeFailureSummary) {
        this.volumeFailureSummary = volumeFailureSummary;
    }
}
