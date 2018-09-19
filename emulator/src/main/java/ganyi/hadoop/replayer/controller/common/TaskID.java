package ganyi.hadoop.replayer.controller.common;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class TaskID extends ApplicationId {

    int id;
    long timeStamp;

    TaskID(int id, long timeStamp) {
        this.id = id;
        this.timeStamp = timeStamp;
    }

    public static TaskID newInstance(int id, long timeStamp) {
        return new TaskID(id, timeStamp);
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    protected void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timeStamp).append(":").append(id);
        return sb.toString();
    }

    @Override
    public long getClusterTimestamp() {
        return timeStamp;
    }

    @Override
    protected void setClusterTimestamp(long clusterTimestamp) {
        this.timeStamp = clusterTimestamp;
    }

    @Override
    protected void build() {

    }
}
