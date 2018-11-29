package cn.itcast.bigdata.weblog.mrbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//将pageview的数据封装在pageVistBean
public class PageVistBean implements Writable {
      private String session;      //session
      private String remot_addr;  //ip地址，远程地址
      private String timestr;      //访问时间
      private String request;      //请求路径
      private int step;            //在同一个session中的步骤
      private String staylong;     //页面停留时长
      private String referal;      //从哪里跳转过来的
      private String useragent;    //客户端详情
      private String bytes_send;   //服务端相应的流量大小
      private String status;        //相应状态码

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getRemot_addr() {
        return remot_addr;
    }

    public void setRemot_addr(String remot_addr) {
        this.remot_addr = remot_addr;
    }

    public String getTimestr() {
        return timestr;
    }

    public void setTimestr(String timestr) {
        this.timestr = timestr;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getStaylong() {
        return staylong;
    }

    public void setStaylong(String staylong) {
        this.staylong = staylong;
    }

    public String getReferal() {
        return referal;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    public String getUseragent() {
        return useragent;
    }

    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    public String getBytes_send() {
        return bytes_send;
    }

    public void setBytes_send(String bytes_send) {
        this.bytes_send = bytes_send;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return
                session + '\001' +
                remot_addr + '\001' +
                timestr + '\001' +
                request + '\001' +
                step +'\001'+
                staylong + '\001' +
                referal + '\001' +
                useragent + '\001' +
                bytes_send + '\001' +
                status + '\001'
                ;
    }

    //写
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.session);
        dataOutput.writeUTF(this.remot_addr);
        dataOutput.writeUTF(this.timestr);
        dataOutput.writeUTF(this.request);
        dataOutput.writeInt(this.step);
        dataOutput.writeUTF(this.staylong);
        dataOutput.writeUTF(this.referal);
        dataOutput.writeUTF(this.useragent);
        dataOutput.writeUTF(this.bytes_send);
        dataOutput.writeUTF(this.status);
    }

    //读
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.session = dataInput.readUTF();
        this.remot_addr = dataInput.readUTF();
        this.timestr = dataInput.readUTF();
        this.request = dataInput.readUTF();
        this.step=dataInput.readInt();
        this.staylong = dataInput.readUTF();
        this.referal = dataInput.readUTF();
        this.useragent = dataInput.readUTF();
        this.bytes_send = dataInput.readUTF();
        this.status = dataInput.readUTF();
    }
}
