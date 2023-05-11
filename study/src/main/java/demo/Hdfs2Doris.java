package demo;


import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


/**
 * @version 1.0.0
 * @title: Hdfs2Doris
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-05-05 17:28
 */


public class Hdfs2Doris {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setParallelism(1);
        String hdfs = "hdfs://192.168.1.21:9000/cassandraData/data_store_collectiondoubledata.csv";
        DataStreamSource<String> source = env.readTextFile(hdfs);
//        DataStreamSource<String> source = env.fromElements("d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:14.377,21534,ea5d2f876123417593b8714091206eb2,0,1663216994377,1,0,1",
//                "4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:15.818,21537,27b918863ba44c7895714cd7f6c148d5,0,1663216995818,1,0,1",
//                "d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:17.270,21540,1283b877b2fe43179a7b16bb211de04b,0,1663216997270,1,0,1",
//                "d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:18.728,21542,0d155cdc53284e38b19dfbea83699436,0,1663216998728,1,0,1",
//                "d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:20.181,21545,4a7c5034579c406fb285a0f1aa9c7922,0,1663217000181,1,0,1",
//                "d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:21.618,21548,e0432b83bd5441f09c7c62b33055ea67,0,1663217001618,1,0,1",
//                "d4e2ee90da2f919badbcc8077e1a9e4d,043a10be68438f8b451febedbe7583fa,UA,2022-09-15 04:43:23.072,21550,1f12229e7f77423f8fab3c89b2f52244,0,1663217003072,1,0,1");

//        source1.print();

        DateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.sss");

        SingleOutputStreamOperator<RowData> stream = source.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String s) throws Exception {
                String[] splits = s.split(",");
                GenericRowData genericRowData = new GenericRowData(12);
//                Timestamp timestamp = new Timestamp(format.parse(splits[3]).getTime());
                TimestampData timestamp = TimestampData.fromEpochMillis(format.parse(splits[3]).getTime());
//                Date date = new Date(timestamp.getMillisecond());
                Integer res = Math.toIntExact(timestamp.getMillisecond() / (1000 * 3600 * 24));

                genericRowData.setField(0, timestamp);
                genericRowData.setField(1, res);
                genericRowData.setField(2, StringData.fromString(splits[0]));
                genericRowData.setField(3, StringData.fromString(splits[1]));
                genericRowData.setField(4, StringData.fromString(splits[2]));
                genericRowData.setField(5, Double.valueOf(splits[4]));
                genericRowData.setField(6, StringData.fromString(splits[5]));
                genericRowData.setField(7, Integer.valueOf(splits[6]));
                genericRowData.setField(8, Long.valueOf(splits[7]));
                genericRowData.setField(9, StringData.fromString(splits[8]));
                genericRowData.setField(10, Integer.valueOf(splits[9]));
                genericRowData.setField(11, StringData.fromString(splits[10]));

                return genericRowData;
            }
        });

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("192.168.1.8:8030")
                .setTableIdentifier("default_cluster:asd.test")
                .setUsername("root")
                .setPassword("")

        ;

        // json format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("doris.batch.size", "2");

//        properties.setProperty("disable_stream_load_2pc", "false");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
//                .setCheckInterval(1000)
//                .setBufferCount(2)
                .disable2PC()
                .setLabelPrefix("label-dorixx") //streamload label prefix
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {
                "createtime", "createdate",
                "devtypeid", "devid",
                "attributecode", "doubledata", "id",
                "isdelete", "origin", "quality",
                "recordtype", "unit"};
        DataType[] types = {
                DataTypes.TIMESTAMP(), DataTypes.DATE(),
                DataTypes.VARCHAR(255), DataTypes.VARCHAR(255),
                DataTypes.VARCHAR(255), DataTypes.DOUBLE(), DataTypes.VARCHAR(255),
                DataTypes.INT(), DataTypes.BIGINT(), DataTypes.VARCHAR(255),
                DataTypes.INT(), DataTypes.VARCHAR(255)};

        builder

                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("json")           //json format
                        .setFieldType(types)
                        .build())
                .setDorisOptions(dorisBuilder.build())
        ;


        stream.sinkTo(builder.build());
//        stream.print();
        env.execute();

    }
}
