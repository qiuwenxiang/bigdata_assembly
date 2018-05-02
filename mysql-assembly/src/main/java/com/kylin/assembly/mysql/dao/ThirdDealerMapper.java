package com.kylin.assembly.mysql.dao;


import com.kylin.assembly.mysql.entity.ThirdDealer;

import java.util.Map;

public interface ThirdDealerMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(ThirdDealer record);

    int insertSelective(ThirdDealer record);

    ThirdDealer selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(ThirdDealer record);

    int updateByPrimaryKey(ThirdDealer record);

    void saveRecordByBrand(Map<String, Object> params);

    void saveRecordByBrandPojo(Map<String, Object> params);
}