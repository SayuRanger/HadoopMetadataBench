package ganyi.hadoop.replayer.controller.common;

import ganyi.hadoop.replayer.message.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class TaskScheduler {
    public int baseParallelFactor;
    public int num_dn;
    TaskResourceManager resManager;
    Queue<ExecutionTuple> execQueue;
    HashMap<String, Integer> progressBar;
    HashMap<String, String> progressBarForPortion;

    String scheduleScript;
    String scheduleFolder;
    Message.TaskType taskType;
    TaskScheduler(String script, String scheduleFolder, Message.TaskType taskType) {
        scheduleScript = script;
        this.scheduleFolder = scheduleFolder;
        execQueue = new LinkedList<>();
        progressBar = new HashMap<>();
        progressBarForPortion = new HashMap<>();
        this.taskType = taskType;
        init();
    }

    void init() {
        //check schedule folder
        resManager = new TaskResourceManager(taskType);
        resManager.loadScheduleFolder(scheduleFolder);

        //load script file
        loadJobScript(scheduleScript);
        baseParallelFactor = resManager.parallelFactor;
        num_dn = resManager.num_dn;
    }

    public ExecutionTuple getNextExeTuple() {
        return execQueue.poll();
    }

    void parseScriptLine(String line) {
        //System.out.println("Parse line [" + line + "]");
        String[] ss = line.split(" ");
        String[] exess = ss[0].split("\\.");
        String[] condss = ss[1].split("\\.");
        ExecutionTuple tuple = new ExecutionTuple();
        tuple.execFile = exess[0];
        if (exess[1].equalsIgnoreCase("all")) {
            tuple.execArg = String.valueOf(resManager.getFileNumbyType(tuple.execFile));
            tuple.execType = EXEC_TYPE.MULTIPLE;
        } else if (exess[1].equalsIgnoreCase("single")) {
            tuple.execArg = String.valueOf(1);
            tuple.execType = EXEC_TYPE.SINGLE;
        } else {
            tuple.execArg = exess[1].replaceAll("\\[|\\]", "");
            tuple.execType = EXEC_TYPE.PORTION;
        }

        tuple.condFile = condss[0];
        if (condss[1].equalsIgnoreCase("all")) {
            tuple.condArg = String.valueOf(resManager.getFileNumbyType(tuple.condFile));
            tuple.condType = EXEC_TYPE.MULTIPLE;
        } else if (condss[1].equalsIgnoreCase("single")) {
            tuple.condArg = "1";
            tuple.condType = EXEC_TYPE.SINGLE;
        } else if (condss[1].equalsIgnoreCase("first")) {
            tuple.condArg = "1";
            tuple.condType = EXEC_TYPE.MULTIPLE;
        } else if (condss[1].equalsIgnoreCase("last")) {
            tuple.condArg = String.valueOf(resManager.getFileNumbyType(tuple.condFile));
            tuple.condType = EXEC_TYPE.MULTIPLE;
        } else {
            tuple.condArg = condss[1].replaceAll("\\[|\\]", "");
            tuple.condType = EXEC_TYPE.PORTION;
        }

        if (tuple.execType == EXEC_TYPE.PORTION &&
                !progressBarForPortion.containsKey(tuple.execFile)) {
            progressBarForPortion.put(tuple.execFile, "");
        }
        else if(tuple.execType!=EXEC_TYPE.PORTION &&
                !progressBar.containsKey(tuple.execFile)){
            progressBar.put(tuple.execFile, 0);
        }

        if (tuple.condType == EXEC_TYPE.PORTION &&
                !progressBarForPortion.containsKey(tuple.condFile)) {
            progressBarForPortion.put(tuple.condFile, "");
        }
        else if(tuple.condType!=EXEC_TYPE.PORTION &&
                !progressBar.containsKey(tuple.condFile)){
            progressBar.put(tuple.condFile, 0);
        }
        //System.out.println("generate tuple: "+tuple);
        execQueue.add(tuple);
    }

    void loadJobScript(String jobScript) {
        try {
            FileReader reader = new FileReader(jobScript);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                if (line.toLowerCase().contains("dn.all")) {
                    continue;
                }
                if (line.toLowerCase().contains("nm.all")) {
                    continue;
                }
                parseScriptLine(line);
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String findAvailable(String type) {
        return resManager.findAvailable(type);
    }

    public void updateProgress(String file) {
        if (progressBar.containsKey(file)) {
            /*System.out.println("TaskScheduler.updateProgress: update file "+file);*/
            progressBar.put(file, progressBar.get(file) + 1);
        } else {
            throw new RuntimeException("no such key " + file + " in progressBar.");
        }
    }

    public void updateProgressForPortion(String file, String pos) {
        if (progressBarForPortion.containsKey(file)) {
            progressBarForPortion.put(file, pos);
        } else {
            throw new RuntimeException("no such key " + file + " in progressBarForPortion.");
        }
    }

    public boolean isConditionReached(ExecutionTuple tuple) {
        String fileName = tuple.condFile;
        EXEC_TYPE type = tuple.condType;

        //System.out.println("Current tuple: " + tuple);
        if (type == EXEC_TYPE.MULTIPLE || type == EXEC_TYPE.SINGLE) {
            int received = progressBar.get(fileName);
            int num = Integer.valueOf(tuple.condArg);
            if (received < num) {
                return false;
            } else {
                //System.out.println("[M/S] Reach condition: ");
                return true;
            }
        } else {
            //PORTION
            String cond = progressBarForPortion.get(fileName);
            if (cond.equals(tuple.condArg)) {
                //System.out.println("[PORTION] Reach condition: ");
                return true;
            } else {
                //System.out.println("[PORTION] fail to pass condition.");
                return false;
            }
        }

    }

    public enum EXEC_TYPE {
        PORTION,
        SINGLE,
        MULTIPLE,
    }

    public class ExecutionTuple {
        public EXEC_TYPE execType;
        public String execFile;
        public String execArg;

        public EXEC_TYPE condType;
        public String condFile;
        public String condArg;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Execution <").append(execFile).append(",").append(execType)
                    .append(",").append(execArg).append(">, condition <").append(condFile)
                    .append(",").append(condType).append(",").append(condArg).append(">.");
            return sb.toString();
        }
    }
}
