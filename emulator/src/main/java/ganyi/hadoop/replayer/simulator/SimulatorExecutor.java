package ganyi.hadoop.replayer.simulator;

public interface SimulatorExecutor {
    void SetupRpcManager();

    void startRPCService();

    void periodicalJob(Simulator.TimerTaskType type, String[] cmd);

    void stopSimulatorRPCConnection();
}
