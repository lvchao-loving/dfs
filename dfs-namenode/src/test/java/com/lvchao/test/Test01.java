package com.lvchao.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @Title: Test01
 * @Package: com.lvchao.test
 * @Description:
 * @auther: chao.lv
 * @date: 2021/11/14 13:18
 * @version: V1.0
 */
public class Test01 {
    public static void main(String[] args) {
        Person person = new Person();
        person.setAge(23);
        person.setName("吕超");
        System.out.println(JSON.toJSONString(person));
        System.out.println(JSONObject.toJSONString(person));
    }
}
class Person{
    private String name;
    private Integer age;
    private String address;

    public Person() {
    }

    public Person(String name, Integer age, String address) {
        this.name = name;
        this.age = age;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public Integer getAge() {
        return age;
    }

    public String getAddress() {
        return address;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}
