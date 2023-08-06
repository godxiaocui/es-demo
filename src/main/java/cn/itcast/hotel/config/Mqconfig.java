package cn.itcast.hotel.config;

import cn.itcast.hotel.constants.HotelMqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Mqconfig {
    // 1。 定义一个交换机。持久化为1，自动删除为false
    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(HotelMqConstants.EXCHANGE_NAME,true,false);
    }
    // 2。 定义一个删除队列。持久化为1
    @Bean
    public Queue deleteQueue(){
        return new Queue(HotelMqConstants.DELETE_QUEUE_NAME,true);
    }
    // 2。 定义一个插入队列。持久化为1
    @Bean
    public Queue insertQueue(){
        return new Queue(HotelMqConstants.INSERT_QUEUE_NAME,true);
    }

    //3. 绑定队列和交换机的关系
    @Bean
    public Binding insertQueueBuinding(){
        return BindingBuilder.bind(insertQueue()).to(topicExchange()).with(HotelMqConstants.INSERT_KEY);
    }
    //3. 绑定队列和交换机的关系
    @Bean
    public Binding deleteQueueBuinding(){
        return BindingBuilder.bind(deleteQueue()).to(topicExchange()).with(HotelMqConstants.DELETE_KEY);
    }

}
