package ganyi.hadoop.replayer.simulator.nn;

public class SharedCondCounter {
    private final Object lock = new Object();
    private int count;

    public SharedCondCounter() {
        count = 0;
    }

    public void inc() {
        synchronized (lock) {
            count += 1;
        }
    }

    public void dec() {
        synchronized (lock) {
            count -= 1;
        }
    }

    public synchronized int get() {
        synchronized (lock) {
            return count;
        }
    }
}

