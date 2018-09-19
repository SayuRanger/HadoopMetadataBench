package ganyi.hadoop.replayer.network;

import java.net.InetSocketAddress;

public class netAddress {
    private String ip;
    private int port;

    netAddress() {
    }

    public netAddress(String ip, String port) {
        this.ip = ip;
        this.port = Integer.parseInt(port);
    }
    public netAddress(String IPandPort) {
        String[] ss = IPandPort.split(":");
        ip = ss[0];
        port = Integer.parseInt(ss[1]);
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ip).append(":").append(port);
        return sb.toString();
    }

    public InetSocketAddress toInetSock() {
        return new InetSocketAddress(ip, port);
    }
}
