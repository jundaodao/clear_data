package cn.itcast.bigdata.weblog.mrbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WebLogBean implements Writable {
    /*
        0、数据是否合法
    *   1、访客ip地址：   58.215.204.118
        2、访客用户信息：  - -
        3、请求时间：[18/Sep/2013:06:51:35 +0000]
        4、请求方式：GET
        5、请求的url：/wp-includes/js/jquery/jquery.js?ver=1.10.2
        6、请求所用协议：HTTP/1.1
        7、响应码：304
        8、返回的数据流量：0
        9、访客的来源url：http://blog.fens.me/nodejs-socketio-chat/
        10、访客所用浏览器：Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0
    * */
    private Boolean valid=true; //数据是否合法
    private String remot_addr;  //访客ip地址
    private String remote_user; //访客用户信息
    private String time_local;  //请求时间与时区
    private String request;     //请求的url与http协议
    private String status;      //请求的状态码
    private String body_bytes_sent; //发送给客户端的数据大小
    private String http_referer;//访客来源，从哪个页面链接过来
    private String http_user_agent;//记录客户浏览器相关

    public WebLogBean() {
    }

    public WebLogBean(Boolean valid, String remot_addr, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent) {
        this.valid = valid;
        this.remot_addr = remot_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
    }



    public Boolean getValid() {
        return valid;
    }

    public void setValid(Boolean valid) {
        this.valid = valid;
    }

    public String getRemot_addr() {
        return remot_addr;
    }

    public void setRemot_addr(String remot_addr) {
        this.remot_addr = remot_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid).append("\001");
        sb.append(this.remot_addr).append("\001");
        sb.append(this.remote_user).append("\001");
        sb.append(this.time_local).append("\001");
        sb.append(this.request).append("\001");
        sb.append(this.status).append("\001");
        sb.append(this.body_bytes_sent).append("\001");
        sb.append(this.http_referer).append("\001");
        sb.append(this.http_user_agent);
        return sb.toString();
    }
    //涉及io操作的写
    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeBoolean(this.valid);
         dataOutput.writeUTF(null==this.remot_addr?"":remot_addr);
         dataOutput.writeUTF(null==remote_user?"":remote_user);
         dataOutput.writeUTF(null==time_local?"":time_local);
         dataOutput.writeUTF(null==request?"":request);
         dataOutput.writeUTF(null==status?"":status);
         dataOutput.writeUTF(null==body_bytes_sent?"":body_bytes_sent);
         dataOutput.writeUTF(null==http_referer?"":http_referer);
         dataOutput.writeUTF(null==http_user_agent?"":http_user_agent);
    }
    //读
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.valid = dataInput.readBoolean();
        this.remot_addr = dataInput.readUTF();
        this.remote_user = dataInput.readUTF();
        this.time_local = dataInput.readUTF();
        this.request = dataInput.readUTF();
        this.status = dataInput.readUTF();
        this.body_bytes_sent = dataInput.readUTF();
        this.http_referer = dataInput.readUTF();
        this.http_user_agent = dataInput.readUTF();
    }
}
