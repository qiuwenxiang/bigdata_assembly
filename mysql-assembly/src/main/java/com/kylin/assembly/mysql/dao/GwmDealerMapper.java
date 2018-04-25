package com.kylin.assembly.mysql.dao;


import com.kylin.assembly.mysql.entity.GwmDealer;

public interface GwmDealerMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(GwmDealer record);

    int insertSelective(GwmDealer record);

    GwmDealer selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(GwmDealer record);

    int updateByPrimaryKey(GwmDealer record);
}