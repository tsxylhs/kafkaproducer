package sparktest;

 


import java.util.Properties;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducer {

    private final Producer<String, String> producer;
    public final static String TOPIC = "websocket";

    private KafkaProducer(){
        Properties props = new Properties();
        //�˴����õ���kafka�Ķ˿�
        props.put("metadata.broker.list", "192.168.3.252:9092");

        //����value�����л���
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //����key�����л���
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() {
        int messageNo = 10;
        final int COUNT = 10000;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
               int data1= (int) Math.round(Math.random() * 1000);
                 String data=Integer.toString(data1);
            producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));
            System.out.println(data);
            messageNo ++;
         
            
        }
    }
    void  produce1(){
    	for(int i=1;i<=10;i++){
    		Scanner sc=new Scanner(System.in);
    		System.out.println("������");
    		String work=sc.next();
    		producer.send(new KeyedMessage<String,String>(TOPIC,work));
    		
    		
    	}
    }

    public static void main( String[] args )
    {
      KafkaProducer kf= new KafkaProducer();
      kf.produce();
      
      
    }
	}
