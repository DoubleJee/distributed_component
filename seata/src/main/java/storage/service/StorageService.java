package storage.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import storage.mapper.Storage;
import storage.mapper.StorageMapper;

@Service
public class StorageService {

    @Autowired
    private StorageMapper storageMapper;

    /**
     * 扣除存储数量
     */
    public void deduct(String commodityCode, int count){
        QueryWrapper<Storage> wrapper = new QueryWrapper<>();
        wrapper.eq("commodity_code",commodityCode);
        Storage storage = storageMapper.selectOne(wrapper);
        storage.setCount(storage.getCount() - count);
        storageMapper.updateById(storage);
    }

}
