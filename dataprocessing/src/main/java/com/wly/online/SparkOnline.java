package com.wly.online;

import com.atguigu.model.StartupReportLogs;
import com.atguigu.model.UserBehaviorStatModel;
import com.atguigu.utils.DateUtils;
import com.atguigu.utils.JSONUtil;
import com.atguigu.utils.MyStringUtil;
import com.atguigu.utils.PropertiesUtil;
import com.wly.service.BehaviorStatService;
import com.wly.util.UserHourPackageKey;
import javafx.util.Duration;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.tools.ConsumerOffsetChecker;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class SparkOnline {
    public static void main(String[] args) throws Exception {
        final Properties serverProperties = PropertiesUtil.getProperties("config.properties");
        String checkpoint = serverProperties.getProperty("streaming.checkpoint.path");
        JavaStreamingContext sparkContext = JavaStreamingContext.getOrCreate(checkpoint, createContext(serverProperties));
        sparkContext.start();
        sparkContext.awaitTermination();
    }

    private static Function0<JavaStreamingContext> createContext(final Properties serverProperties) {
        Function0<JavaStreamingContext> contextFunc = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                final String topics = serverProperties.getProperty("kafka.topic");
                HashSet topicSet = new HashSet(Arrays.asList(topics.split(",")));
                String brokers = serverProperties.getProperty("kafka.broker.list");
                final String groupId = serverProperties.getProperty("kafka.groupId");
                String checkpoint = serverProperties.getProperty("streaming.checkpoint.path");
                String interval = serverProperties.getProperty("streaming.interval");
                long streamInterval = Long.parseLong(interval);
                HashMap<String, String> kafkaParm = new HashMap<>();
                kafkaParm.put("metadata.broker.list", brokers);
                kafkaParm.put("group.id",groupId);

                final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParm);
                Map<TopicAndPartition,Long> offset=getConsumerOffsets(kafkaCluster, groupId, topicSet);

                SparkConf sparkConf = new SparkConf().setAppName("pipi").setMaster("loacl[*]");
                sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator");
                sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100");

                JavaStreamingContext javaStreamingContext =
                        new JavaStreamingContext(sparkConf, Durations.seconds(streamInterval));
                JavaInputDStream<String> directStream = KafkaUtils.createDirectStream(javaStreamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParm,
                        offset,
                        new Function<MessageAndMetadata<String, String>, String>() {
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        }
                );

                final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

                JavaDStream<String> kafkaMessageDstreamTramsform = directStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsetRanges1 = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsetRanges1);
                        return rdd;
                    }
                });

                final JavaDStream<String> messageFilter = kafkaMessageDstreamTramsform.filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String message) throws Exception {
                        try {
                            if (!message.contains("activeTimeInMs") && !message.contains("stayDurationInSec") &&
                                    !message.contains("errorMajor")) {
                                return false;
                            }

                            if (!message.contains("activeTimeInMs") || !message.contains("appVersion")) {
                                return false;
                            }
                            StartupReportLogs startupReportLog = JSONUtil.json2Object(message, StartupReportLogs.class);
                            if (startupReportLog == null ||
                                    MyStringUtil.isEmpty(startupReportLog.getAppVersion()) ||
                                    MyStringUtil.isEmpty(startupReportLog.getUserId()) ||
                                    MyStringUtil.isEmpty(startupReportLog.getAppId())
                                    ) {
                                return false;
                            }
                            return true;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                });

                JavaPairDStream<UserHourPackageKey, Long> key21l = messageFilter.mapToPair(new PairFunction<String, UserHourPackageKey, Long>() {
                    @Override
                    public Tuple2<UserHourPackageKey, Long> call(String s) throws Exception {
                        StartupReportLogs log = JSONUtil.json2Object(s, StartupReportLogs.class);
                        UserHourPackageKey userHourPackageKey = new UserHourPackageKey();
                        userHourPackageKey.setCity(log.getCity());
                        userHourPackageKey.setHour(DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, log.getStartTimeInMs()));
                        userHourPackageKey.setPackageName(log.getAppId());
                        userHourPackageKey.setUserId(Long.parseLong(log.getUserId().substring(4)));
                        return Tuple2.apply(userHourPackageKey, 1l);
                    }
                });
                JavaPairDStream<UserHourPackageKey, Long> userHourPackageKeyLongJavaPairDStream = key21l;
                JavaPairDStream<UserHourPackageKey, Long> key2count = key21l.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                key2count.foreachRDD(new VoidFunction<JavaPairRDD<UserHourPackageKey, Long>>() {
                    @Override
                    public void call(JavaPairRDD<UserHourPackageKey, Long> userHourPackageKeyLongJavaPairRDD) throws Exception {
                       userHourPackageKeyLongJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<UserHourPackageKey, Long>>>() {
                           @Override
                           public void call(Iterator<Tuple2<UserHourPackageKey, Long>> iterator) throws Exception {
                               BehaviorStatService behaviorStatService=BehaviorStatService.getInstance(serverProperties);
                               while(iterator.hasNext()){
                                   Tuple2<UserHourPackageKey, Long> t = iterator.next();
                                   UserHourPackageKey userHourPackageKey = t._1;
                                   UserBehaviorStatModel model=new UserBehaviorStatModel();
                                   model.setCity(userHourPackageKey.getCity());
                                   model.setHour(userHourPackageKey.getHour());
                                   model.setPackageName(userHourPackageKey.getPackageName());
                                   model.setTimeLen(t._2);
                                   model.setUserId(MyStringUtil.getFixedLengthStr(userHourPackageKey.getUserId()+"",10));

                                   behaviorStatService.addUserNumOfCity(model);
                               }

                           }
                       });
                        offsetToZk(kafkaCluster, offsetRanges, groupId);
                    }
                });

                return javaStreamingContext;
            }

        };
        return contextFunc;
    }

    private static void offsetToZk(KafkaCluster kafkaCluster, AtomicReference<OffsetRange[]> offsetRanges, String groupId) {
        for (OffsetRange o:offsetRanges.get()){
            TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
            HashMap<TopicAndPartition, Object> topicAndPartitionObjectHashMap = new HashMap<>();
            topicAndPartitionObjectHashMap.put(topicAndPartition, o.untilOffset());
            scala.collection.mutable.Map<TopicAndPartition,Object> scalamap= JavaConversions.mapAsScalaMap(topicAndPartitionObjectHashMap);
            scala.collection.immutable.Map immutableMap=scalamap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                @Override
                public Tuple2<TopicAndPartition,Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                    return v1;
                }
            });
            kafkaCluster.setConsumerOffsets(groupId, immutableMap);
        }
    }

    public static Map<TopicAndPartition,Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, HashSet topicSet) {
        //将java的set结构转换为scala的mutable.set结构
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        //将scala的mutable.set结构转换为immutable.set结构
        scala.collection.immutable.Set<String> immutableTopics=mutableTopics.toSet();
        //根据传入的分区，获取TopicAndPartition形式的返回数据
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2=(scala.collection.immutable.Set<TopicAndPartition>)
                kafkaCluster.getPartitions(immutableTopics).right().get();

        //创建用于存储offset数据的hashMap
        //(TopicAntPartition,offset)
        Map<TopicAndPartition, Long> consumerOffsetLong = new HashMap<>();

        if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetLong.put(topicAndPartition, 0l);
            }
        } else {
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    (scala.collection.immutable.Map<TopicAndPartition, Object>)
                            kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for(TopicAndPartition topicAndPartition:topicAndPartitionSet1){
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetLong.put(topicAndPartition, offset);
            }
        }

        return consumerOffsetLong;
    }

    public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        // 将Java的HashMap转化为Scala的mutable.Map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParams);
        // 将Scala的mutable.Map转化为imutable.Map
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                        return v1;
                    }
                });

        // 由于KafkaCluster的创建需要传入Scala.HashMap类型的参数，因此要进行上述的转换
        // 将immutable.Map类型的Kafka参数传入构造器，创建KafkaCluster
        return new KafkaCluster(scalaKafkaParam);
    }
}
