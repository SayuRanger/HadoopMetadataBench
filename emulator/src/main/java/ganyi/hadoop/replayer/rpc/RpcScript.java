package ganyi.hadoop.replayer.rpc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RpcScript {

    List<RpcPosition> indexList;
    List<RpcValueSet> valList;

    private int terminatingPoint;
    private int cursor;

    public RpcScript() {
        indexList = new ArrayList<>();
        valList = new ArrayList<>();

    }

    public int getCursor() {
        return cursor;
    }

    public void setCursor(int cursor) {
        this.cursor = cursor;
    }

    public void setTerminatingPoint(RpcPosition position) {
        terminatingPoint = indexList.indexOf(position) + 1;
    }

    public void setCursorRange(String start, String end) {
        if (start.equalsIgnoreCase("")) {
            setCursor(0);
        } else {
            setCursor(indexList.indexOf(new RpcPosition(start)));
        }

        if (end.equalsIgnoreCase("")) {
            terminatingPoint = valList.size();
        } else {
            setTerminatingPoint(new RpcPosition(end));
        }
    }

    public String getPos(int index) {
        return indexList.get(index).toString();
    }

    public String getTerminatingPoint() {
        return getPos(terminatingPoint - 1);
    }

    public void setTerminatingPoint(int pos) {
        terminatingPoint = pos;
    }

    public void loadScript(String file) {
        cursor = 0;
        try {
            FileReader reader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] csvLine = RpcInterpreter.parseCSVLine(line);
                /*System.out.println("Parse result:");
                for (String s : csvLine) {
                    System.out.println(s);
                }*/
                String pos = csvLine[0].trim();
                String rpc = csvLine[1].trim();
                String timediff = csvLine[2].trim();
                String template = "";
                String valueDic = "";
                if (csvLine.length == 5) {
                    template = csvLine[3].trim();
                    valueDic = csvLine[4].trim();
                }
                RpcPosition position = new RpcPosition(pos);
                indexList.add(position);
                RpcValueSet valueSet = new RpcValueSet(rpc, timediff, template, valueDic);
                valList.add(valueSet);
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    public String[] getNext() {
        if (cursor >= terminatingPoint) {
            return null;
        }
        String[] ss = new String[5];
        ss[0] = indexList.get(cursor).toString();
        ss[1] = valList.get(cursor).name;
        ss[2] = valList.get(cursor).timediff;
        ss[3] = valList.get(cursor).template;
        ss[4] = valList.get(cursor).valueDic;
        cursor += 1;
        return ss;
    }

    public String[] getNextByPos(String pos) {
        RpcPosition position = new RpcPosition(pos);
        int index = indexList.indexOf(position);
        if (index >= valList.size()) {
            System.out.printf("Error: index value %d is out of boundary\n", index);
            System.exit(-1);
        }
        String[] ss = new String[5];
        ss[0] = indexList.get(index).toString();
        ss[1] = valList.get(index).name;
        ss[2] = valList.get(index).timediff;
        ss[3] = valList.get(index).template;
        ss[4] = valList.get(index).valueDic;
        return ss;
    }

    public class RpcValueSet {
        public String name;
        public String template;
        public String valueDic;
        public String timediff;

        public RpcValueSet(String name, String timediff, String template, String valueDic) {
            this.name = name;
            this.timediff = timediff;
            this.template = template;
            this.valueDic = valueDic;
        }

    }
}
