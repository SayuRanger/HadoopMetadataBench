package ganyi.hadoop.replayer;

import ganyi.hadoop.replayer.simulator.SimTimer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;


public class OnlyForExperiment implements Runnable {

    public static void main(String[] args) {


        /*final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        SimTimer simTimer= new SimTimer();
        Log LOG = LogFactory.getLog(simTimer.getClass());
        Runnable task1 = ()->{
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(dtf.format(LocalDateTime.now())+" task11111");
        };
        Runnable task2 = ()->{
            System.out.println(dtf.format(LocalDateTime.now())+" task11111");
        };
        simTimer.scheduleOnce(2000,task1);
        OnlyForExperiment only = new OnlyForExperiment();
        Thread nt = new Thread(only);
        nt.start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("before join");
        try {
            nt.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("after join");*/
        //simTimer.cancel();
    }



    @Override
    public void run() {
        SimTimer simTimer = new SimTimer();
        Runnable task = ()->{
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("over.");
        };
        simTimer.scheduleOnce(6000,task);
    }
}


