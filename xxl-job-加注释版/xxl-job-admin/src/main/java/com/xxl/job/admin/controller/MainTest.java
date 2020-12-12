package com.xxl.job.admin.controller;

import java.io.File;

public class MainTest {
    public static void main(String[] args){
        try {
            File f = new File("d:/");
        }catch (Exception e){
            System.out.print(e.getStackTrace());

        }
    }
}
