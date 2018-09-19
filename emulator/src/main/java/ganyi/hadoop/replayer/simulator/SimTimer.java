package ganyi.hadoop.replayer.simulator;



public class SimTimer implements Runnable {
    Thread timerThread;
    Boolean isRunning;
    long interval;
    Runnable task;
    boolean runOnce;
    boolean aggresive = false;
    long maxInterval;

    public SimTimer(){
        isRunning = new Boolean(false);
        runOnce = false;
    }

    public void cancel(){
        if(isRunning == false){
            throw new RuntimeException("Timer is already cancelled.");
        }
        synchronized (isRunning){
            isRunning = false;
        }

        try {
            timerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void schedule(long interval,Runnable task ){
        aggresive = false;
        isRunning =true;
        this.interval = interval;
        this.task = task;
        timerThread = new Thread(this);
        timerThread.start();
    }

    public void scheduleOnce(long interval, Runnable task){
        runOnce = true;
        isRunning = true;
        this.interval = interval;
        this.task = task;
        timerThread = new Thread(this);
        timerThread.start();
    }

    public void aggresiveSchedule(long initial, long maxInterval, Runnable task){
        aggresive = true;
        isRunning = true;
        this.interval = initial;
        this.maxInterval = maxInterval;
        this.task = task;
        timerThread = new Thread(this);
        timerThread.start();
    }

    void aggresiveRoutine(){
        while (true){
            synchronized (isRunning){
                if(isRunning == false){
                    break;
                }
            }
            long startTime = System.currentTimeMillis();
            task.run();
            long duration = System.currentTimeMillis() - startTime;
            if(duration < interval){
                try {
                    Thread.sleep(interval - duration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                if (interval < maxInterval) {
                    try {
                        Thread.sleep(interval *2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    try {
                        Thread.sleep(interval /2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
            if (interval < maxInterval){
                interval *=2;
            }
            if (runOnce){
                isRunning = false;
            }
        }
    }

    void normalRoutine(){
        while (true){
            synchronized (isRunning){
                if(isRunning == false){
                    break;
                }
            }
            long startTime = System.currentTimeMillis();
            task.run();
            long duration = System.currentTimeMillis() - startTime;
            if(duration < interval){
                try {
                    Thread.sleep(interval - duration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    Thread.sleep(interval/2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (runOnce){
                isRunning = false;
            }
        }
    }

    @Override
    public void run() {
        if(aggresive){
            aggresiveRoutine();
        }
        else{
            normalRoutine();
        }
    }
}
