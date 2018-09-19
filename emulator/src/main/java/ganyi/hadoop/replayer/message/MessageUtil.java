package ganyi.hadoop.replayer.message;

import java.io.Serializable;

public class MessageUtil implements Serializable {
    private MessageUtil() {
    }

    public static Message decode(String msg) {
        String[] ss = msg.split("\\$");
        Message message = new Message(Message.MSG_TYPE.valueOf(ss[0]), ss[1], ss[2], ss[3], Long.valueOf(ss[5]));
        message.setSrc_pool(ss[4]);
        return message;
    }

    public static String encode(Message message) {
        StringBuilder sb = new StringBuilder("");
        sb.append(message.getMsgType().toString()).append("$")
                .append(message.getCmd()).append("$")
                .append(message.getSrc()).append("$")
                .append(message.getDest()).append("$")
                .append(message.getSrc_pool()).append("$")
                .append(message.getJobID());
        return sb.toString();
    }
}
