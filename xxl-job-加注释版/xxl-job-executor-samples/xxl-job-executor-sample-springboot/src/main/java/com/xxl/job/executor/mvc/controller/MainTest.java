package com.xxl.job.executor.mvc.controller;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MainTest {

    public static void main(String[] args){
        Map<String, String> linkedHashMap = new LinkedHashMap<>();
        linkedHashMap.put("name1", "josan1");
        linkedHashMap.put("name2", "josan2");
        linkedHashMap.put("name3", "josan3");
        Set<Map.Entry<String, String>> set = linkedHashMap.entrySet();
        Iterator<Map.Entry<String, String>> iterator = set.iterator();
//        while(iterator.hasNext()) {
//            Map.Entry entry = iterator.next();
//            String key = (String) entry.getKey();
//            String value = (String) entry.getValue();
//            System.out.println("key:" + key + ",value:" + value);
//        }
        String a = iterator.next().getKey();
        System.out.print(a);
    }
}
