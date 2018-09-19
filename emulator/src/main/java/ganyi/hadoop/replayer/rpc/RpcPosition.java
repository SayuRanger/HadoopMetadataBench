package ganyi.hadoop.replayer.rpc;

/**
 * Created by ganyi on 4/16/2017.
 */
public class RpcPosition {
    int segment;
    int iteration;
    int offset;


    public RpcPosition(String str) {
        String[] ss = str.split(",");
        segment = Integer.valueOf(ss[0]);
        iteration = Integer.valueOf(ss[1]);
        offset = Integer.valueOf(ss[2]);
    }

    public RpcPosition(int segment, int iteration, int offset) {
        this.segment = segment;
        this.iteration = iteration;
        this.offset = offset;
    }

    RpcPosition(String segment, String iteration, String offset) {
        this.segment = Integer.valueOf(segment);
        this.iteration = Integer.valueOf(iteration);
        this.offset = Integer.valueOf(offset);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(segment).append(",").append(iteration).append(",")
                .append(offset);
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof RpcPosition)) return false;
        return equals((RpcPosition) other);
    }

    public boolean equals(RpcPosition position) {
        return equals(position.segment, position.iteration, position.offset);
    }

    /*public boolean equals(String str){
        return equals(new RpcPosition(str));
    }*/

    boolean equals(int segment, int iteration, int offset) {
        return this.segment == segment && this.iteration == iteration && this.offset == offset;
    }
}
