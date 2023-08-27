package com.lc.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName:WriterIotdb_JAVA
 * Package:com.lc.iotdb
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-08-26-23:18
 */
@SuppressWarnings({"squid:S106", "squid:S1144"})
public class WriterIotdb_JAVA {

        private static SessionPool sessionPool;

    private static Session session;


    private static void constructCustomSession () {
        session =
                new Session.Builder()
                        .host("192.168.161.101")
                        .username("root")
                        .password("root")
                        .build();
    }

        private static void constructCustomSessionPool () {
            sessionPool =
                    new SessionPool.Builder()
                            .host("192.168.161.101")
                            .port(6667)
                            .user("root")
                            .password("root")
                            .maxSize(3)
                            .build();
        }

    public static void main(String[] args) throws IoTDBConnectionException {
        Map<String, TSDataType> measureTSTypeInfos = new HashMap<>();
        measureTSTypeInfos.put("s1", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s2", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s3", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s4", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s5", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s6", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s7", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s8", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s9", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s10", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s11", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s12", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s13", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s14", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s15", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s16", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s17", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s18", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s19", TSDataType.TEXT);
        measureTSTypeInfos.put("s20", TSDataType.TEXT);

        List<MeasurementSchema> schemas = new ArrayList<>();
        measureTSTypeInfos.forEach((mea, type) -> schemas.add(new MeasurementSchema(mea, type)));


        Tablet tablet = new Tablet("root.sg2.test", schemas,1000000);

        tablet.rowSize=1000000;

        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000000; i++) {

            tablet.addTimestamp(i,i);
            tablet.addValue("s1",i,0.23);
            tablet.addValue("s2",i,0.23);
            tablet.addValue("s3",i,0.23);
            tablet.addValue("s4",i,0.23);
            tablet.addValue("s5",i,0.23);
            tablet.addValue("s6",i,0.23);
            tablet.addValue("s7",i,0.23);
            tablet.addValue("s8",i,0.23);
            tablet.addValue("s9",i,0.23);
            tablet.addValue("s10",i,0.23);
            tablet.addValue("s11",i,0.23);
            tablet.addValue("s12",i,0.23);
            tablet.addValue("s13",i,0.23);
            tablet.addValue("s14",i,0.23);
            tablet.addValue("s15",i,0.23);
            tablet.addValue("s16",i,0.23);
            tablet.addValue("s17",i,0.23);
            tablet.addValue("s18",i,0.23);
            tablet.addValue("s19",i,"hello");
            tablet.addValue("s20",i,"hello");

        }

        try {
            constructCustomSessionPool();

            sessionPool.insertTablet(tablet,true);

            long end = System.currentTimeMillis();

            System.out.println(end-start);

        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        } catch (StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        sessionPool.close();


    }



}
