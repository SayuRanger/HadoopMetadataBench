package ganyi.hadoop.replayer.rpc.param;

import org.json.JSONObject;

public class BlockReportContextParam {
    private int totalRpcs;
    private int curRpc;
    private long reportId;

    public static BlockReportContextParam parseJson(JSONObject object) {
        BlockReportContextParam param = new BlockReportContextParam();
        param.setTotalRpcs(UtilJson.getInt(object, "totalRpcs"));
        param.setCurRpc(UtilJson.getInt(object, "curRpc"));
        param.setReportId(UtilJson.getLong(object, "id"));
        return param;
    }

    public int getTotalRpcs() {
        return totalRpcs;
    }

    public void setTotalRpcs(int totalRpcs) {
        this.totalRpcs = totalRpcs;
    }

    public int getCurRpc() {
        return curRpc;
    }

    public void setCurRpc(int curRpc) {
        this.curRpc = curRpc;
    }

    public long getReportId() {
        return reportId;
    }

    public void setReportId(long reportId) {
        this.reportId = reportId;
    }
}
