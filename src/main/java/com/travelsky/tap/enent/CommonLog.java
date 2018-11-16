package com.travelsky.tap.enent;

/**
 * Created by pczhangyu on 2018/11/13.
 */
public class CommonLog {

    private Long timeStamp;

    private String fileName;

    private String nodeIp;

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    @Override
    public String toString() {
        return "CommonLog{" +
                "timeStamp=" + timeStamp +
                ", fileName='" + fileName + '\'' +
                ", nodeIp='" + nodeIp + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
