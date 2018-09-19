package ganyi.hadoop.replayer;

import ganyi.hadoop.replayer.network.netAddress;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalConfigure {

    //Key: simulator ID/CC ID, value: ip&port
    private ConcurrentHashMap<String, netAddress> ProcessIpAddressMap;
    private ConcurrentHashMap<String, String> simulatorMap;      //Simulator ID -> CC / SimPool ID
    private InetAddress localInetAddress;
    private String CCID;
    private HashMap<String, String> SettingMap;


    private GlobalConfigure() {
        ProcessIpAddressMap = new ConcurrentHashMap<>();
        simulatorMap = new ConcurrentHashMap<>();
        SettingMap = new HashMap<>();
        updateSimulatorMap("CC.1", "CC.1");
        CCID = "CC.1";
    }

    public GlobalConfigure(String file, String identifier) {
        this();
        setupConfig(file);
        updateSimulatorMap(identifier, identifier);
        //showConfig();
    }

    public GlobalConfigure(String file) {
        this();
        setupConfig(file);
        showConfig();
    }

    public String getCCID() {
        return CCID;
    }

    public List<String> getSimulatorPoolSet() {
        List<String> list = new ArrayList<>();
        for (String str : ProcessIpAddressMap.keySet()) {
            if (str.toLowerCase().contains("simpool")) {
                list.add(str);
            }
        }
        return list;
    }

    public void showConfig() {
        System.out.println("simulatorMap");
        /*for(Map.Entry<String, String> entry: simulatorMap.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println("---------------------------------\nsimulatorMap");*/
        for (Map.Entry<String, netAddress> entry : ProcessIpAddressMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        for (Map.Entry<String, String> entry : SettingMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println("end.\n------------------------------");
    }

    public void setupConfig(String fileName) {
        try {
            FileReader reader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(reader);
            if (!ProcessIpAddressMap.isEmpty()) {
                ProcessIpAddressMap.clear();
            }
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                } else if (line.trim().startsWith(">")) {
                    String[] ss = line.substring(1).split("=");
                    SettingMap.put(ss[0].trim(), ss[1].trim());
                    continue;
                }
                String[] strs = line.split(" ");
                String[] str2s = strs[1].split(":");
                ProcessIpAddressMap.put(strs[0], new netAddress(str2s[0], str2s[1]));
                updateSimulatorMap(strs[0], strs[0]);
            }
            bufferedReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            setLocalInetAddress(InetAddress.getLocalHost());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return;
    }

    public String getSetting(String key) {
        if (SettingMap.containsKey(key)) {
            return SettingMap.get(key);
        } else {
            return null;
        }
    }

    public String getSetting(String key, String defualt) {
        return getSetting(key) != null ? getSetting(key) : defualt;
    }

    public void updateSimulatorMap(String key, String value) {
        if (!simulatorMap.containsKey(key)) {
            simulatorMap.put(key, value);
        }
    }

    public String genSimulatorMapString(String sim) {
        StringJoiner sj = new StringJoiner("#");
        for (Map.Entry<String, String> entry : simulatorMap.entrySet()) {
            if (entry.getKey().split("\\.")[0].equalsIgnoreCase(sim)) {
                sj.add(entry.getKey().split("\\.")[1]
                        + ":" + entry.getValue().split("\\.")[1]);
            }
        }
        return sj.toString();
    }

    public void deleteSimPool(String simPool) {
        simulatorMap.values().remove(simPool);
        ProcessIpAddressMap.remove(simPool);
    }

    public void parseSimulatorGenString(String msg, String sim) {
        String[] ss = msg.split("#");
        for (String str : ss) {
            String[] kv = str.split(":");
            updateSimulatorMap(sim + "." + kv[0], "simpool." + kv[1]);
        }
        return;
    }

    public void deleteSimulatorMapEntry(String id) {
        if (simulatorMap.containsKey(id)) {
            simulatorMap.remove(id);
        } else {
            System.out.printf("No such entry key %s in global configure\n", id);
        }
    }

    public InetAddress getLocalInetAddress() {
        return localInetAddress;
    }

    public void setLocalInetAddress(InetAddress localInetAddress) {
        this.localInetAddress = localInetAddress;
    }

    public String getSimPoolID(String ID) {
        String simPoolID = simulatorMap.get(ID);
        if (simPoolID == null) {
            System.out.println("Cannot find value in simulatorMap by key " + ID);
        }
        return simPoolID;
    }

    public netAddress getAddr(String ID) {
        String simPoolID = simulatorMap.get(ID);
        if (simPoolID == null) {
            System.out.printf("Cannot find key " + ID + " in GlobalConfigure.simulatorMap.\n");
            return null;
        }
        netAddress ret = ProcessIpAddressMap.get(simPoolID);
        if (ret == null) {
            System.out.printf("Cannot find key " +
                    simulatorMap.get(ID) + " in GlobalConfigure.ProcessIpAddressMap.\n");
            return null;
        }
        return ret;
    }

    public netAddress getNameNodeAddr() {
        return ProcessIpAddressMap.get("NN.1");
    }

    public netAddress getResourceManagerAddr() {
        return ProcessIpAddressMap.get("RM.1");
    }

    public netAddress getCentrolControllerAddr() {
        return ProcessIpAddressMap.get("CC.1");
    }

}
