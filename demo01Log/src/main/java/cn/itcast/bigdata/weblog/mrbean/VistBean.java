package cn.itcast.bigdata.weblog.mrbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VistBean implements Writable {
    private String session;       //session
    private String remote_addr;   //访问地址
    private String inTime;        //第一次访问的时间
    private String outTime;       //最后一次访问的时间
    private String inPage;        //第一个访问的页面
    private String outPage;       //最后一次访问的页面
    private String referal;       //从哪里跳转过来的
    private int pageVisits;

    public int getPageVisits() {
        return pageVisits;
    }

    public void setPageVisits(int pageVisits) {
        this.pageVisits = pageVisits;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getInTime() {
        return inTime;
    }

    public void setInTime(String inTime) {
        this.inTime = inTime;
    }

    public String getOutTime() {
        return outTime;
    }

    public void setOutTime(String outTime) {
        this.outTime = outTime;
    }

    public String getInPage() {
        return inPage;
    }

    public void setInPage(String inPage) {
        this.inPage = inPage;
    }

    public String getOutPage() {
        return outPage;
    }

    public void setOutPage(String outPage) {
        this.outPage = outPage;
    }

    public String getReferal() {
        return referal;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    @Override
    public String toString() {
        return
                 session + '\001' +
                 remote_addr + '\001' +
                 inTime + '\001' +
                 outTime + '\001' +
                 inPage + '\001' +
                 outPage + '\001' +
                 referal + '\001'+
                 pageVisits
                ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeUTF(session);
         dataOutput.writeUTF(remote_addr);
         dataOutput.writeUTF(inTime);
         dataOutput.writeUTF(outTime);
         dataOutput.writeUTF(inPage);
         dataOutput.writeUTF(outPage);
         dataOutput.writeUTF(referal);
         dataOutput.writeInt(pageVisits);
    }
     //读
    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.session= dataInput.readUTF();
         this.remote_addr= dataInput.readUTF();
         this.inTime= dataInput.readUTF();
         this.outTime= dataInput.readUTF();
         this.inPage= dataInput.readUTF();
         this.outPage= dataInput.readUTF();
         this.referal= dataInput.readUTF();
         this.pageVisits=dataInput.readInt();
    }
}
