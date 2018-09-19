package ganyi.hadoop.replayer.message;

import java.util.LinkedList;
import java.util.Queue;

public class MessageQueue<T> {
    //int capacity = -1;
    private Queue<T> queue = null;

    public MessageQueue() {
        queue = new LinkedList<>();
    }

    /*public MessageQueue(int capacity) {
        queue = new LinkedList<>();

    }*/
    public synchronized T get() {
        try {
            while (queue.isEmpty()) {
                wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        T message = queue.remove();
        //notify();
        return message;
    }

    public synchronized void put(T obj) {
        /*try {
            while (queue.size()!=-1 && queue.size() ==capacity ){
                wait();
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }*/
        queue.add(obj);
        notify();
        return;
    }

    public int getQueueSize(){
        return queue.size();
    }
}
