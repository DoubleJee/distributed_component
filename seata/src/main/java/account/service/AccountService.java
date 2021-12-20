package account.service;

import account.mapper.Account;
import account.mapper.AccountMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class AccountService {

    @Autowired
    private AccountMapper accountMapper;

    /**
     * 从用户账户中借出
     */
    public void debit(String userId, int money){
        QueryWrapper<Account> wrapper = new QueryWrapper<>();
        wrapper.eq("user_id",userId);
        Account account = accountMapper.selectOne(wrapper);
        account.setMoney(account.getMoney().subtract(BigDecimal.valueOf(money)));
        accountMapper.updateById(account);
    }
}
