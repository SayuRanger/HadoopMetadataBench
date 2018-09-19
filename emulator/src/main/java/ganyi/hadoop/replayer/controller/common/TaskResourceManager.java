package ganyi.hadoop.replayer.controller.common;

import ganyi.hadoop.replayer.message.Message;

import java.io.File;
import java.util.*;


public class TaskResourceManager {
    String scheduleFolder;
    int parallelFactor;
    int num_dn;
    Random random;
    Message.TaskType taskType;
    HashMap<String, List<String>> fileCollection;
    HashMap<String, List<FILE_TYPE>> fileType;
    HashMap<String, List<Integer>> available;
    TaskResourceManager(Message.TaskType taskType) {
        fileCollection = new HashMap<>();
        fileType = new HashMap<>();
        available = new HashMap<>();
        random = new Random(System.currentTimeMillis());
        this.taskType = taskType;
    }

    void show() {
        for (Map.Entry<String, List<String>> entry : fileCollection.entrySet()) {
            System.out.println("Entry: " + entry.getKey() + "<" + fileType.get(entry.getKey()).get(0) + ">");
            for (String s : entry.getValue()) {
                System.out.print(s + " ");
            }
            System.out.print("\n");
        }
    }

    int getFileNumbyType(String type) {
        return fileCollection.get(type).size();
    }

    String findAvailable(String type) {
        List<Integer> refList = available.get(type);
        if (refList.size() == 0) {
            return null;
        }
        int randi = random.nextInt(refList.size());
        int index = refList.remove(randi);
        if (fileType.get(type).get(index) == FILE_TYPE.SINGLE) {
            return fileCollection.get(type).get(index);
        } else if (fileType.get(type).get(index) == FILE_TYPE.MULTIPLE) {
            return type + "." + fileCollection.get(type).get(index);
        } else {
            //error
        }
        return "";
    }

    void loadScheduleFolder(String folderName) {
        scheduleFolder = folderName;
        File directory = new File(folderName);
        File[] fileList = directory.listFiles();
        for (File file : fileList) {
            String[] ss = file.getName().split("\\.");
            if (ss.length == 2) {
                String fname = ss[0];
                if (fileCollection.containsKey(fname)) {
                    //Error
                } else {
                    List<String> collectionList = new ArrayList<>();
                    List<FILE_TYPE> stateList = new ArrayList<>();
                    List<Integer> availableList = new ArrayList<>();
                    collectionList.add(ss[0]);
                    stateList.add(FILE_TYPE.SINGLE);
                    availableList.add(collectionList.indexOf(ss[0]));
                    fileCollection.put(fname, collectionList);
                    fileType.put(fname, stateList);
                    available.put(fname, availableList);
                }
            } else if (ss.length == 3) {
                String fname = ss[0];
                if (fileCollection.containsKey(fname)) {
                    List list = fileCollection.get(fname);
                    list.add(ss[1]);
                    List stateList = fileType.get(fname);
                    stateList.add(FILE_TYPE.MULTIPLE);
                    int index = list.indexOf(ss[1]);
                    available.get(ss[0]).add(index);
                } else {
                    List<String> collectionList = new ArrayList<>();
                    List<FILE_TYPE> stateList = new ArrayList<>();
                    List<Integer> availableList = new ArrayList<>();
                    collectionList.add(ss[1]);
                    stateList.add(FILE_TYPE.MULTIPLE);
                    availableList.add(collectionList.indexOf(ss[1]));
                    fileCollection.put(fname, collectionList);
                    fileType.put(fname, stateList);
                    available.put(fname, availableList);
                }
            } else {   //error

            }
        }
        if (taskType == Message.TaskType.NameNode) {
            num_dn = fileCollection.get("dn").size();
        } else if (taskType == Message.TaskType.ResourceManager) {
            num_dn = fileCollection.get("nm").size();
        }
        parallelFactor = num_dn - 1;
    }

    enum FILE_TYPE {
        UNKNOWN,
        SINGLE,
        MULTIPLE,
    }
}
