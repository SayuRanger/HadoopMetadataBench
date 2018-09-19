package ganyi.hadoop.replayer.controller;

import ganyi.hadoop.replayer.controller.common.CentralController;
import ganyi.hadoop.replayer.controller.common.TaskExecutor;
import ganyi.hadoop.replayer.controller.common.TaskScheduler;
import ganyi.hadoop.replayer.message.Message;
import ganyi.hadoop.replayer.message.MessageQueue;
import ganyi.hadoop.replayer.simulator.nn.ClientSimulator;

public class NameNodeTaskExecutor extends TaskExecutor {
    int parallel_num;

    public NameNodeTaskExecutor(String[] envs, MessageQueue<Message> in, MessageQueue<Message> out, CentralController parent) {
        taskType = Message.TaskType.NameNode;
        init(envs, in, out, parent);
        parallel_num = (int)Math.ceil(userDefineParallelFactor * scheduler.baseParallelFactor);
    }

    @Override
    public void run() {
        runNameNodeTask();
    }

    private void runNameNodeTask() {
        TaskScheduler.ExecutionTuple tuple;
        while ((tuple = scheduler.getNextExeTuple()) != null) {
            executeTuple(tuple);
            //then wait for response and check if condition is satisfied.
            while (!scheduler.isConditionReached(tuple)) {
                Message msg = pendingMessageQueue.get();
                LOG.info("[" + jobID + "] Receive message: " + msg);
                if (msg.getMsgType() == Message.MSG_TYPE.finish_response) {
                    String cmd = msg.getCmd();
                    if (!cmd.contains("#")) { //Process response from simulators those executes whole script.
                        String[] ss = cmd.split("/");
                        String type = ss[ss.length - 1].split("\\.")[0];
                        //if type is mapper/reducer, send release message.
                        if (type.toLowerCase().contains("mapper") ||
                                type.toLowerCase().contains("reducer")) {
                            //TODO: add thread replace logic to reduce thread_create/release overhead.
                            String simPoolID = configure.getSimPoolID(msg.getSrc());
                            Message releaseMsg = new Message(Message.MSG_TYPE.release_simulator,
                                    msg.getSrc(),
                                    getIdentifier(),
                                    simPoolID, jobID);
                            parent.sendTCPMessage(releaseMsg);

                            String available = scheduler.findAvailable(type);
                            if (available != null) {
                                String simID = available + "|" + jobID;
                                Message createMsg = new Message(Message.MSG_TYPE.create_simulator,
                                        simID, getIdentifier(), simPoolID, jobID,taskType);
                                createMsg.setMisc(ClientSimulator.CLIENT_TYPE.other.name());
                                parent.sendTCPMessage(createMsg);
                                configure.updateSimulatorMap(simID, simPoolID);
                                Message newTaskMsg = new Message(Message.MSG_TYPE.command,
                                        "start#" + scheduleFolder + simID.split("\\|")[0] + ".csv",
                                        getIdentifier(), simID, jobID);
                                parent.sendTCPMessage(newTaskMsg);
                            } else {
                                //Error
                            }
                        } else {
                            String simPoolID = configure.getSimPoolID(msg.getSrc());
                            Message releaseMsg = new Message(Message.MSG_TYPE.release_simulator,
                                    msg.getSrc(), getIdentifier(), simPoolID, jobID);
                            parent.sendTCPMessage(releaseMsg);
                        }
                        scheduler.updateProgress(type);
                    } else {//Process response from simulators executing 'portion'.
                        String[] tmpss = cmd.split("#");
                        String pos = tmpss[1];
                        String[] ss = tmpss[0].split("/");
                        String type = ss[ss.length - 1].split("\\.")[0];
                        scheduler.updateProgressForPortion(type, pos);
                    }
                } else if (msg.getMsgType() == Message.MSG_TYPE.create_simulator) {
                    String id = msg.getCmd();
                    String simPoolID = msg.getSrc();
                    configure.updateSimulatorMap(id, simPoolID);
                } else if (msg.getMsgType() == Message.MSG_TYPE.release_simulator) {
                    String id = msg.getCmd();
                    configure.deleteSimulatorMapEntry(id);
                    simulatorList.remove(id);
                }
                else {
                    LOG.error("receive a message sent to wrong place.\tMessage: " + msg);
                }
            }
        }
        destroy();
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        respondWithFinish();
        while (true) {
            Message message = pendingMessageQueue.get();
            if (message.getMsgType() == Message.MSG_TYPE.release_jobExecutor) {
                break;
            }
        }
    }

    @Override
    protected void executeTuple(TaskScheduler.ExecutionTuple tuple) {
        LOG.info(">>>>> Executing tuple: " + tuple + " right now.");
        String filePrefix = tuple.execFile;
        TaskScheduler.EXEC_TYPE type = tuple.execType;
        if (type == TaskScheduler.EXEC_TYPE.MULTIPLE &&
                (filePrefix.toLowerCase().contains("mapper") ||
                        filePrefix.toLowerCase().contains("reducer"))) {
            int num = Integer.parseInt(tuple.execArg);
            num = num < parallel_num ? num : parallel_num;
            //num = num<2*scheduler.baseParallelFactor?num:2*scheduler.baseParallelFactor;
            initMultipleSimulator(filePrefix, num, ClientSimulator.CLIENT_TYPE.other.name());
        } else if (type == TaskScheduler.EXEC_TYPE.MULTIPLE) {
            int num = Integer.parseInt(tuple.execArg);
            initMultipleSimulator(filePrefix, num, ClientSimulator.CLIENT_TYPE.other.name());
        } else if (type == TaskScheduler.EXEC_TYPE.SINGLE) {
            initMultipleSimulator(filePrefix, 1, ClientSimulator.CLIENT_TYPE.other.name());
        } else if (type == TaskScheduler.EXEC_TYPE.PORTION) {
            //check if filePrefix already in configure.
            String simPoolID = configure.getSimPoolID(filePrefix + "|" + jobID);
            String simID;
            if (simPoolID == null) {
                simPoolID = getNextSimpoolID();
                simID = scheduler.findAvailable(filePrefix) + "|" + jobID;

                configure.updateSimulatorMap(simID, simPoolID);
                simulatorList.add(simID);

                //scheduler.resetProgress(simID);
                Message msg = new Message(Message.MSG_TYPE.create_simulator,
                        simID, getIdentifier(), simPoolID, jobID, taskType);
                msg.setMisc(ClientSimulator.CLIENT_TYPE.appMaster.name());
                parent.sendTCPMessage(msg);

                Message anothermsg = new Message(Message.MSG_TYPE.command,
                        "start#" + scheduleFolder + simID.split("\\|")[0] + ".csv#" + tuple.execArg,
                        getIdentifier(), simID, jobID);
                parent.sendTCPMessage(anothermsg);
            } else {
                simID = filePrefix + "|" + jobID;
                //scheduler.resetProgress(simID);
                Message msg = new Message(Message.MSG_TYPE.command,
                        "start#" + scheduleFolder + simID.split("\\|")[0] + ".csv#" + tuple.execArg,
                        getIdentifier(), simID, jobID);
                parent.sendTCPMessage(msg);
            }
        } else {
            //Error
        }
    }
}
