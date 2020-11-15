package com.itheima.report.bean;

import java.util.Objects;

/**
 * @author zhangYu
 * @date 2020/11/15
 */
public class AdClickLog {

    /**
     * 广告id
     */
    private long t_id;
    /**
     * 广告主ID
     */
    private long corpurin;
    /**
     * 域名，用户从哪里跳过来的
     */
    private String host;
    /**
     * 设备类型，pc,mobile,other
     */
    private String device_type;
    /**
     * 广告来源
     */
    private String ad_source;
    /**
     * 广告媒介
     */
    private String ad_media;
    /**
     * 广告系列
     */
    private String ad_campaign;
    /**
     * 城市
     */
    private String city;
    /**
     * 时间(1.点击时间；2.页面跳转时间)
     */
    private String timestamp;
    /**
     * 用户ID
     */
    private String user_id;
    /**
     * 点击时用户ID，用户如果点击该字段才会有值，否则"null"
     */
    private String click_user_id;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdClickLog that = (AdClickLog) o;
        return t_id == that.t_id &&
                corpurin == that.corpurin &&
                Objects.equals(host, that.host) &&
                Objects.equals(device_type, that.device_type) &&
                Objects.equals(ad_source, that.ad_source) &&
                Objects.equals(ad_media, that.ad_media) &&
                Objects.equals(ad_campaign, that.ad_campaign) &&
                Objects.equals(city, that.city) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(user_id, that.user_id) &&
                Objects.equals(click_user_id, that.click_user_id);
    }

    @Override
    public String toString() {
        return "AdClickLog{" +
                "t_id=" + t_id +
                ", corpurin=" + corpurin +
                ", host='" + host + '\'' +
                ", device_type='" + device_type + '\'' +
                ", ad_source='" + ad_source + '\'' +
                ", ad_media='" + ad_media + '\'' +
                ", ad_campaign='" + ad_campaign + '\'' +
                ", city='" + city + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", user_id='" + user_id + '\'' +
                ", click_user_id='" + click_user_id + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(t_id, corpurin, host, device_type, ad_source, ad_media, ad_campaign, city, timestamp, user_id, click_user_id);
    }

    public long getT_id() {
        return t_id;
    }

    public void setT_id(long t_id) {
        this.t_id = t_id;
    }

    public long getCorpurin() {
        return corpurin;
    }

    public void setCorpurin(long corpurin) {
        this.corpurin = corpurin;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getAd_source() {
        return ad_source;
    }

    public void setAd_source(String ad_source) {
        this.ad_source = ad_source;
    }

    public String getAd_media() {
        return ad_media;
    }

    public void setAd_media(String ad_media) {
        this.ad_media = ad_media;
    }

    public String getAd_campaign() {
        return ad_campaign;
    }

    public void setAd_campaign(String ad_campaign) {
        this.ad_campaign = ad_campaign;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getClick_user_id() {
        return click_user_id;
    }

    public void setClick_user_id(String click_user_id) {
        this.click_user_id = click_user_id;
    }
}
