package com.kylin.assembly.mysql.entity;

public class ThirdDealer {
    private Integer id;

    private String dealer_name;

    private String phone;

    private String address;

    private String zbx;

    private String zby;
    private String city;

    private String brand_type;

    private Integer dealer_id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDealer_name() {
        return dealer_name;
    }

    public void setDealer_name(String dealer_name) {
        this.dealer_name = dealer_name;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getZbx() {
        return zbx;
    }

    public void setZbx(String zbx) {
        this.zbx = zbx;
    }

    public String getZby() {
        return zby;
    }

    public void setZby(String zby) {
        this.zby = zby;
    }

    public String getBrand_type() {
        return brand_type;
    }

    public void setBrand_type(String brand_type) {
        this.brand_type = brand_type;
    }

    public Integer getDealer_id() {
        return dealer_id;
    }

    public void setDealer_id(Integer dealer_id) {
        this.dealer_id = dealer_id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}