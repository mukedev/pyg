package com.itheima.report.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * //@RestController ：其中所有方法都带有一个@ResponseBody
 * @author zhangYu
 * @date 2020/10/31
 */
@RestController
public class TestController {

    @RequestMapping("/test")
    public String test(String json) {
        System.out.println(json);
        return json;
    }

}
