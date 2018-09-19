package ganyi.hadoop.replayer.rpc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONObject;

import java.io.IOException;

public interface RPCExecutor {
    Gson gson = new GsonBuilder().create();

    void metricsReport();

    void updateLatency(long t1, long t2, String pos, String cmd);

    void loadScript(String file);

    void testRPC(String[] cmds);

    void playOnce(String pos) throws IOException;

    void play(String range) throws IOException;

    void playbook() throws IOException;

    void respondWithFinish(String content);

    void ActionStation(String pos, String rpc, String timediff, String jsonString) throws IOException;

    String ExecuteRPC(String pos, String cmd, JSONObject object) throws IOException, YarnException;
}
