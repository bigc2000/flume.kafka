package com.tutu.flume.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.junit.Test;
/**
 * test not yet
 * @author Free
 *
 */
public class KafkaSinkTest {

    @Test
    public void testConfigure() {
        KafkaSink sink = new KafkaSink();
        Context context = new Context();
        Properties prop = new Properties();
        InputStream in=null;
        try {
            in = KafkaSinkTest.class.getClassLoader().getResourceAsStream("server.properties");
            prop.load(in);
            for(Entry<?,?>e:prop.entrySet()){
                if(e.getKey() !=null && e.getValue()!=null){
                    context.put(e.getKey().toString(), e.getValue().toString());
                }
            }
            IOUtils.closeQuietly(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sink.configure(context);
        //context.put(KafkaSink.KAFKA_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

}
