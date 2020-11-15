package com.itheima.report.util;

import com.alibaba.fastjson.JSONObject;
import com.itheima.report.bean.AdClickLog;
import com.itheima.report.bean.ClickLog;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 点击流日志模拟器
 */
public class AdClickLogGenerator {
    private static Long[] t_ids = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};
    private static Long[] corpurins = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};
    private static Long[] user_ids = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};

    private static String[] hosts = new String[]{"baidu.com", "google.com"};
    private static String[] ad_sources = new String[]{"ads1", "ads2", "ads3"};
    private static String[] ad_medias = new String[]{"adm1", "adm2", "adm3"};
    private static String[] ad_campaigns = new String[]{"adc1", "adc2", "adc3"};

    /**
     * 设备类型
     */
    private static String[] device_type = new String[]{"pc", "mobile", "other"};

    /**
     * 城市
     */
    private static String[] citys = new String[]{"beijing", "shanghai", "guangzhou", "shenzhen"};


    //获取时间
    public static List<Long[]> producetimes() {
        List<Long[]> usetimelog = new ArrayList<Long[]>();
        for (int i = 0; i < 100; i++) {
            Long[] timesarray = gettimes("2018-12-12 24:60:60:000");
            usetimelog.add(timesarray);
        }
        return usetimelog;
    }

    private static Long[] gettimes(String time) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);
            long timetemp = date.getTime();
            Random random = new Random();
            int randomint = random.nextInt(10);
            long starttime = timetemp - randomint * 3600 * 1000;
            long endtime = starttime + randomint * 3600 * 1000;
            return new Long[]{starttime, endtime};
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Long[]{0l, 0l};
    }


    /**
     * 模拟发送Http请求到上报服务系统
     *
     * @param url
     * @param json
     */
    public static void send(String url, String json) {
        try {
            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            JSONObject response = null;
            try {
                StringEntity s = new StringEntity(json.toString(), "utf-8");
                s.setContentEncoding("utf-8");
                // 发送json数据需要设置contentType
                s.setContentType("application/json");
                post.setEntity(s);

                HttpResponse res = httpClient.execute(post);
                if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    // 返回json格式：
                    String result = EntityUtils.toString(res.getEntity());
                    System.out.println(result);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            //频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
            AdClickLog clickLog = new AdClickLog();

            clickLog.setT_id(t_ids[random.nextInt(t_ids.length)]);
            clickLog.setCity(citys[random.nextInt(citys.length)]);
            clickLog.setAd_source(ad_sources[random.nextInt(ad_sources.length)]);
            clickLog.setAd_media(ad_medias[random.nextInt(ad_medias.length)]);
            clickLog.setAd_campaign(ad_campaigns[random.nextInt(ad_campaigns.length)]);
            clickLog.setCorpurin(corpurins[random.nextInt(corpurins.length)]);
            clickLog.setDevice_type(device_type[random.nextInt(device_type.length)]);
            clickLog.setHost(hosts[random.nextInt(hosts.length)]);
            clickLog.setUser_id(user_ids[random.nextInt(user_ids.length)].toString());
            // 时间需要设置成UTC格式 UTC: "yyyy-MM-dd'T'HH:mm:ss.SSSz"
            final Date date = new Date();
            clickLog.setTimestamp(dateFormat.format(date));
            if (i % 2 == 0) {
                clickLog.setClick_user_id(clickLog.getUser_id());
            }

            String jonstr = JSONObject.toJSONString(clickLog);
            System.out.println(jonstr);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            send("http://localhost:8888/adreceive", jonstr);
        }
    }
}
