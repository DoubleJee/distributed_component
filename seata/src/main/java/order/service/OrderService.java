package order.service;

import io.seata.spring.annotation.GlobalTransactional;
import order.mapper.Order;
import order.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Service
public class OrderService {

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private RestTemplate restTemplate;

    // 下单
    @GlobalTransactional
    public void create(String userId, String commodityCode, int orderCount) {
        String storageUrl = "http://127.0.0.1:8083/deduct";
        restTemplate.getForEntity(storageUrl,Void.class);

        String AccountUrl = "http://127.0.0.1:8081/debit";
        restTemplate.getForEntity(AccountUrl, Void.class);

        int orderMoney = 5;
        Order order = new Order();
        order.setUserId(userId);
        order.setCommodityCode(commodityCode);
        order.setCount(orderCount);
        order.setMoney(BigDecimal.valueOf(orderMoney));
        orderMapper.insert(order);

        int i = 1 / 0;
    }
}
