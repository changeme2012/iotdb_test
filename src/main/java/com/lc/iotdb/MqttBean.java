package com.lc.iotdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName:MqttBean
 * Package:com.lc.iotdb
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-08-26-17:24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MqttBean {
   String device;
   String timestamp;
   String measurements;
   String values;
}
