package ganyi.hadoop.replayer;

import ganyi.hadoop.replayer.controller.common.CentralController;
import ganyi.hadoop.replayer.simulator.SimulatorPool;
import ganyi.hadoop.replayer.simulator.rm.NodeManagerSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerAppMasterSimulator;
import ganyi.hadoop.replayer.simulator.rm.ResourceManagerClientSimulator;

import java.io.IOException;

public class Main {
    public static void main(String[] args)
            throws IOException {
        if (args.length == 0) {
            System.out.printf("Error:\n\tService name must be specified.\n");
            System.exit(-1);
        }


        String service = args[0];
        Thread thread = null;
        if (args.length == 1) {
            if (service.equalsIgnoreCase("ResourceManagerAppMasterSimulator")) {

            } else {
                System.out.printf("Error:\n\tArguments are needed for %s\n", service);
                System.exit(-3);
            }
        } else {
            String service_args[] = new String[args.length - 1];

            for (int i = 1; i < args.length; i++) {
                service_args[i - 1] = args[i];
            }
            if (service.equalsIgnoreCase("CentralController")) {
                thread = new Thread(new CentralController(service_args));
            } else if (service.equalsIgnoreCase("SimulatorPool")) {
                thread = new Thread(new SimulatorPool(service_args));
            }
            //Only use following classes for test.
            else if (service.equalsIgnoreCase("ClientSimulator")) {

            } else if (service.equalsIgnoreCase("DataNodeSimulator")) {

            } else if (service.equalsIgnoreCase("ResourceManagerAppMasterSimulator")) {
                thread = new Thread(new ResourceManagerAppMasterSimulator(service_args));
            } else if (service.equalsIgnoreCase("ResourceManagerClientSimulator")) {
                thread = new Thread(new ResourceManagerClientSimulator(service_args));
            } else if (service.equalsIgnoreCase("NodeManagerSimulator")) {
                thread = new Thread(new NodeManagerSimulator(service_args));
            } /*else if (service.equalsIgnoreCase("ResourceManagerSimulator")) {
                thread = new Thread(new ResourceManagerSimulator(service_args));
            }*/ else {
                System.out.printf("Error:\n\tNo such class %s in replayer\n", service);
                System.exit(-2);
            }

        }

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.printf("Reach last line of main in %s.\n", service);
    }
}
