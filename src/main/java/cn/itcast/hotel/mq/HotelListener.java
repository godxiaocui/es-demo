package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.HotelMqConstants;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HotelListener {
    @Autowired
    IHotelService hotelService;
    //监听酒店新增的业务酒店id
    @RabbitListener(queues = HotelMqConstants.INSERT_QUEUE_NAME)
    public void insert(Long id){
        hotelService.insertByid(id);
    }
    //监听酒店删除的业务酒店id
    @RabbitListener(queues = HotelMqConstants.DELETE_QUEUE_NAME)
    public void DELETE(Long id){
        hotelService.deleteByid(id);
    }
}
