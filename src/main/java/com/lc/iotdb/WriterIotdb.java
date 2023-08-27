package com.lc.iotdb;

import com.alibaba.fastjson2.JSONObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:WriterIotdb
 * Package:com.lc.iotdb
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-08-25-20:24
 */
public class WriterIotdb {
    public static void main(String[] args) throws Exception {

        MQTT mqtt = new MQTT();
        mqtt.setHost("192.168.161.101", 1883);
        mqtt.setUserName("root");
        mqtt.setPassword("root");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        long start = System.currentTimeMillis();

        int count = 0;
        List<String> payloads = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            String payload = String.format("{\n" +
                    "      \"device\":\"root.sg.d1\",\n" +
                    "      \"timestamp\":%d,\n" +
                    "      \"measurements\":[\"s1\",\"s2\",\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\",\"s9\",\"s10\",\"s11\",\"s12\",\"s13\",\"s14\",\"s15\",\"s16\"],\n" +
                    "      \"values\":[0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635]\n" +
                    " }", System.currentTimeMillis());


            JSONObject jsonObject = new JSONObject();
            ArrayList<String> timestamp = new ArrayList<>();
            ArrayList<String> values = new ArrayList<>();
            jsonObject.put("device","root.sg.d1");

            MqttBean mqttBean = new MqttBean();

            mqttBean.setDevice("root.sg.d1");



            for (int j = 0; j < 100; j++) {
                timestamp.add(String.valueOf(System.currentTimeMillis()));

                  }
            jsonObject.put("timestamp",String.valueOf(System.currentTimeMillis()));
            jsonObject.put("values","[0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635,0.530635]");





            payloads.add(payload);


            count++;
            connection.publish("root.sg.d1.s1", payload.getBytes(), QoS.EXACTLY_ONCE, false);
        }

        System.out.println("循环次数:" +count);
        System.out.println("生成数据量:" +payloads.size());
        long end = System.currentTimeMillis();
        System.out.println("花费时间："+(end-start));

        connection.disconnect();






    }
}
