import com.atguigu.utils.PropertiesUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaConsumer {
    private static Configuration configuration = new Configuration();
    private static FileSystem fileSystem = null;
    private static FSDataOutputStream outputStream = null;
    private static Path writePath = null;
    private static String hdfsBasicPath = null;
    private static Properties properties = null;

    static {
        try {
            properties = PropertiesUtil.getProperties("hdfs_consumer.properties");
            hdfsBasicPath = properties.getProperty("hdfsBasicPath");
            fileSystem = FileSystem.get(new URI("hdfs://192.168.1.101:9000"), configuration, properties.getProperty("user"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Properties kafkaproperties = PropertiesUtil.getProperties("kafka_client_consumer.properties");

            //创建消费者连接器
            ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaproperties));

            HashMap<String, Integer> topicCount = new HashMap<>();
            topicCount.put("analysis-test", 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCount);

            KafkaStream<byte[], byte[]> kafkaStream = consumerMap.get("analysis-test").get(0);
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();

            //获取当前时间
            long lastTime = System.currentTimeMillis();

            //获取数据写入的全路径
            String totalPath = getTotalPath(lastTime);

            //根据路径创建Path对象
            writePath = new Path(totalPath);

            //创建文件流
            if (fileSystem.exists(writePath)) {
                outputStream = fileSystem.append(writePath);
            } else {
                outputStream = fileSystem.create(writePath, true);
            }

            while (iterator.hasNext()) {
                //收集1分钟的数据后更换目录
                if (System.currentTimeMillis() - lastTime > 60000) {
                    outputStream.close();
                    long currentTime = System.currentTimeMillis();
                    String newPath = getTotalPath(currentTime);
                    writePath = new Path(newPath);
                    outputStream = fileSystem.create(writePath);
                    lastTime = currentTime;
                }

                String jsonStr = new String(iterator.next().message());

                if(jsonStr.contains("appVersion")){
                    System.out.println("startupPage");
                    save(jsonStr);
                }else if(jsonStr.contains("currentPage")){
                    System.out.println("PageLog");
                }else{
                    System.out.println("ErrorLog");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void save(String log) {
        try {
            //将日志内容写到hdfs文件系统
            String logEnd = log + "\r\n";
            outputStream.write(logEnd.getBytes());
            //通过hflush实现不关闭outputStream就将数据刷新到hdfs文件中
            //hflush仍然有一定的延迟时间，即隔一段时间后，将数据写入hdfs文件中
            outputStream.hflush();
            outputStream.hsync();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取完整的路径名称
    private static String getTotalPath(long lastTime) {
        //时间格式转换
        String formDate = timeTransForm(lastTime);

        //提取目录
        String directory = getDirectoryFromDate(formDate);

        //提取文件名称
        String fileName = getFileName(formDate);

        //全路径
        String totalPath = hdfsBasicPath + directory + "/" + fileName;
        return totalPath;
    }

    private static String getFileName(String date) {
        String[] pathFileName = date.split("-");
        String fileName = pathFileName[2];
        return fileName;
    }

    //组成yyyyMM/dd形式的目录名称
    private static String getDirectoryFromDate(String date) {
        String[] directories = date.split("-");
        String directory = directories[0] + "/" + directories[1];
        return directory;
    }

    // 将毫秒形式的时间转换为yyyyMM-dd-HHmm形式的时间
    private static String timeTransForm(long timeInMills) {
        Date time = new Date(timeInMills);
        String formDate = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd-HHmm");
        formDate = sdf.format(time);
        return formDate;
    }

}
