package ganyi.hadoop.replayer.message;

import java.io.Serializable;

public class Message implements Serializable {
    private MSG_TYPE msgType;
    private String cmd;
    private String src;
    private String dest;
    private String src_pool;
    private Long jobID;
    private TaskType taskType;
    private String misc = "";
    public Message(MSG_TYPE msgType) {
        this(msgType, "_", "_", "_", 0);
    }
    public Message(MSG_TYPE msgType, String cmd, String src, String dest, long jobID) {
        this.cmd = cmd;
        this.msgType = msgType;
        this.src = src;
        this.dest = dest;
        this.jobID = jobID;
        this.taskType = TaskType.None;
    }

    public Message(MSG_TYPE msg_type, String cmd,
                   String src, String dest, long jobID, TaskType taskType) {
        this(msg_type, cmd, src, dest, jobID);
        this.taskType = taskType;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public String getMisc() {
        return misc;
    }

    public void setMisc(String misc) {
        this.misc = misc;
    }

    public Long getJobID() {
        return jobID;
    }

    public void setJobID(Long jobID) {
        this.jobID = jobID;
    }

    public String getSrc_pool() {
        return src_pool;
    }

    public void setSrc_pool(String src_pool) {
        this.src_pool = src_pool;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("From: ").append(src).append(" To: ").append(dest)
                .append(" JobID[").append(jobID).append("] <")
                .append(msgType.toString()).append("> [").append(cmd).append("]");
        if (!taskType.name().equalsIgnoreCase(TaskType.None.toString())) {
            sb.append("|").append(taskType.name()).append("|");
        }
        if (!misc.equalsIgnoreCase("")) {
            sb.append("%").append(misc).append("%");
        }
        return sb.toString();
    }

    public MSG_TYPE getMsgType() {
        return msgType;
    }

    public String getCmd() {
        return cmd;
    }

    public String getDest() {
        return dest;
    }

    public String getSrc() {
        return src;
    }

    public void setRMRPC(String content, String sock) {
        cmd = content;
        src = sock;
    }

    public enum MSG_TYPE {
        command(0),
        create_simulator(1),
        release_simulator(2),
        stop_instance(3),
        finish_response(4),
        stopService(5),
        BRD_command(6),
        BRD_finish(7),
        sync_dn(8),
        sync_nm(9),
        jobExecutor_finish(10),
        release_jobExecutor(11),
        break_Message_dispatch(12),
        start_HB_count(13),
        RM_RPC_CALL(14),        //notify CC that RPC server receive msg from RM
        obtain_appid(15),       //run client gets application id and sends it to CC
        ChangeHBState(16),
        RMAppMasterStatus(17),
        nm_message(18),
        batch_processing(19);

        private int value;
        MSG_TYPE(int value){
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public enum TaskType {
        ResourceManager(2),
        NameNode(1),
        None(0);
        private int value;
        TaskType(int value){
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /*public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public void setType(MSG_TYPE msgType) {
        this.msgType = msgType;
    }*/
}
