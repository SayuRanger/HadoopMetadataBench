package ganyi.hadoop.replayer;

import ganyi.hadoop.replayer.simulator.Simulator;

import java.util.*;

public class MetricsCollector {

    private class Pair<L,R>{
        private L l;
        private R r;

        public Pair(L l,R r){
            this.l = l;
            this.r = r;
        }

        public L getL() {
            return l;
        }

        public R getR() {
            return r;
        }

        public void setL(L l) {
            this.l = l;
        }

        public void setR(R r) {
            this.r = r;
        }

        @Override
        public int hashCode(){
            return l.hashCode()^r.hashCode() ;
        }
    }

    final private Object lock0 = new Object();
    //final private Object lock1 = new Object();
    final private Object lock2 = new Object();


    long interval;

    List<String> rpcName;
    List<Long> rpcLatency;
    List<String> rpcPos;
    List<Long> heartBeatLatency;
    List<Long> renweLeaseLatency;
    long count;
    HashMap<String, Pair<Long,Long>> latencyMap;

    GlobalConfigure configure;

    int extraFactor;

    double latency;

    boolean recordDetailedLatency ;

    public void setConfigure(GlobalConfigure configure) {


    }

    public MetricsCollector(GlobalConfigure configure) {
        rpcName = new ArrayList<>();
        rpcLatency = new ArrayList<>();
        rpcPos = new ArrayList<>();
        count = 0;
        interval = 0;
        extraFactor = 2;
        heartBeatLatency = new ArrayList<>(100);
        renweLeaseLatency = new ArrayList<>(100);


        this.configure = configure;
        String value = configure.getSetting("nnlatmap","false");
        recordDetailedLatency = Boolean.valueOf(value);
        if(recordDetailedLatency) {
            latencyMap = new HashMap<>();
        }
        else{
            latencyMap = null;
        }
    }

    public void updateRpc(String name, String pos, long interval) {
        synchronized (lock0) {
            //rpcName.add(name);
            //rpcPos.add(pos);
            rpcLatency.add(interval);
            count += 1;
            if(recordDetailedLatency) {
                if (latencyMap.containsKey(name)) {
                    Pair<Long, Long> pair = latencyMap.get(name);
                    pair.setL(pair.getL() + 1);
                    pair.setR(pair.getR() + interval);
                    latencyMap.put(name, pair);
                } else {
                    Pair<Long, Long> pair = new Pair<>(1l, interval);
                    latencyMap.put(name, pair);
                }
            }

        }
    }

    public void updateHeartBeat(long latency) {
        heartBeatLatency.add(latency);
        synchronized (lock0) {
            count += 1;
        }

    }

    public void updateRenewLease(long latency) {
        rpcLatency.add(latency);
        renweLeaseLatency.add(latency);
        synchronized (lock0) {
            count += 1;
        }
    }

    public void updateInterval(long t1, long t2) {
        synchronized (lock2) {
            interval = (t2 - t1);
        }
    }

    double aveLatency() {
        double latencySum = 0;
        for (long i : rpcLatency) {
            latencySum += i;
        }
        latency = latencySum / count;
        return latency;
    }

    double throughput() {
        double ret = count;
        return ret / (1.0 * interval / 1000);
    }

    String reportExtraodinaryLatency() {
        StringJoiner sj = new StringJoiner("|");
        for (int i = 0; i < rpcName.size(); i++) {
            //if(rpcLatency.get(i) > latency*extraFactor){
            sj.add(rpcName.get(i) + "%" + rpcPos.get(i) + "%"
                    + rpcLatency.get(i));
        }
        return sj.toString();
    }

    public void report(Simulator simulator) {
        StringBuilder sb = new StringBuilder();
        sb.append("Metrics@");
        sb.append(simulator.getIdentifier()).append("@");
        sb.append(count).append("@");
        sb.append(interval).append("@");
        sb.append(String.format("%.4f", aveLatency())).append("@");
        //sb.append(reportExtraodinaryLatency());
        if(recordDetailedLatency) {
            StringJoiner sj = new StringJoiner("|");
            for (Map.Entry<String, Pair<Long, Long>> entry : latencyMap.entrySet()) {
                String str = "";
                str += entry.getKey();
                str += ":";
                str += String.valueOf(entry.getValue().getR());
                str += ":";
                str += String.valueOf(entry.getValue().getL());
                //str+=String.valueOf(1.0* entry.getValue().getR() / entry.getValue().getL());
                sj.add(str);
            }
            sb.append(sj.toString());
        }
        sb.append("@");
        /*if (heartBeatLatency.size() != 0) {
            sb.append("@");
            StringJoiner sj = new StringJoiner("|");
            for (long i : heartBeatLatency) {
                sj.add(String.valueOf(i));
            }
            sb.append(sj.toString());
        }
        if (renweLeaseLatency.size() != 0) {
            sb.append("@");
            StringJoiner sj = new StringJoiner("|");
            for (long i : renweLeaseLatency) {
                sj.add(String.valueOf(i));
            }
            sb.append(sj.toString());
        }*/
        System.out.println(sb.toString());
    }
}
