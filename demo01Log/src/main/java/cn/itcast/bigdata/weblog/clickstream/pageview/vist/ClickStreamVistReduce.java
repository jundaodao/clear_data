package cn.itcast.bigdata.weblog.clickstream.pageview.vist;

import cn.itcast.bigdata.weblog.mrbean.PageVistBean;
import cn.itcast.bigdata.weblog.mrbean.VistBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class ClickStreamVistReduce extends Reducer<Text,PageVistBean,NullWritable,Text> {
    ArrayList<PageVistBean> beans = new ArrayList<>();
    Text v = new Text();
    /*
    接收到的是以session为聚合因素，相同session的对象放一起
    * */
    @Override
    protected void reduce(Text key, Iterable<PageVistBean> values, Context context) throws IOException, InterruptedException {
        //首先让pagevistBean中的step排序，使用比较器
        for (PageVistBean bean : values) {
            PageVistBean pageVistBean = new PageVistBean();
            //深克隆一下
            try {
                BeanUtils.copyProperties(pageVistBean,bean);
                beans.add(pageVistBean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Collections.sort(beans, new Comparator<PageVistBean>() {
            @Override
            public int compare(PageVistBean o1, PageVistBean o2) {
                if(o1.getStep()>o2.getStep()){
                    return 1;
                }
                return -1;
            }
        });

        //取这次vist的首尾pageviwe记录，将数据放入VistBean中
        VistBean visitBean = new VistBean();
        // 取visit的首记录
        visitBean.setInPage(beans.get(0).getRequest());
        // 第一次访问的时间
        visitBean.setInTime(beans.get(0).getTimestr());
        // 取visit集合当中末尾的记录即可
        visitBean.setOutPage(beans.get(beans.size()-1).getRequest());
        visitBean.setOutTime(beans.get(beans.size()-1).getTimestr());
        //visit访问的页面数,一个session里面的访问页数
        visitBean.setPageVisits(beans.size());
        // 来访者的ip,一个session肯定是相同的ip,所以直接获取就可以
        visitBean.setRemote_addr(beans.get(0).getRemot_addr());
        // 本次visit的referal
        visitBean.setReferal(beans.get(0).getReferal());
        visitBean.setSession(key.toString());
        //输出k3,v3
        v.set(visitBean.toString());
        context.write(NullWritable.get(),v);
    }
}
