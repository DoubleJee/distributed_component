package account.mapper;

import com.baomidou.mybatisplus.extension.activerecord.Model;

import java.math.BigDecimal;

public class Account extends Model<Account> {

    private Integer id;

    private String userId;

    private BigDecimal money;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public BigDecimal getMoney() {
        return money;
    }

    public void setMoney(BigDecimal money) {
        this.money = money;
    }
}