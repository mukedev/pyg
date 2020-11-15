package com.itheima.report.controller;

import com.alibaba.fastjson.JSON;
import com.itheima.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangYu
 * @date 2020/10/31
 */
@RestController
public class AdReportController {

    @Value("${kafka.topic.ad}")
    private String topic;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/adreceive")
    public Map<String, String> receive(@RequestBody String json) {
        Map<String, String> map = new HashMap<>();
        // 构建Message
        try {
            Message msg = new Message();
            msg.setMessage(json);
            msg.setCount(1);
            msg.setTimestamp(System.currentTimeMillis());

            //发送消息到kafka
            kafkaTemplate.send(topic, JSON.toJSONString(msg));
            map.put("success", "true");
        } catch (Exception e) {
            e.printStackTrace();
            map.put("success", "false");
        }
        return map;
    }
}
