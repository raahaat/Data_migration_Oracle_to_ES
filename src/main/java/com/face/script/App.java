package com.face.script;

import java.io.IOException;
import java.sql.SQLException;
import com.mashape.unirest.http.exceptions.UnirestException;


public class App {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, UnirestException, InterruptedException {

        long startTime = System.currentTimeMillis();
        ETLThread tr = new ETLThread(0,72);
        // ETLThread tr1 = new ETLThread(14,14);
        // ETLThread tr2 = new ETLThread(28,14);
        // ETLThread tr3 = new ETLThread(42,14);
        // ETLThread tr4 = new ETLThread(56,16);
        // ETLThread tr5 = new ETLThread(0,72);
        // ETLThread tr6 = new ETLThread(0,72);
        // ETLThread tr7 = new ETLThread(0,72);
        // ETLThread tr8 = new ETLThread(0,72);
        // ETLThread tr9 = new ETLThread(0,72);
        Thread t = new Thread(tr);
        // Thread t1 = new Thread(tr1);
        // Thread t2 = new Thread(tr2);
        // Thread t3 = new Thread(tr3);
        // Thread t4 = new Thread(tr4);
        // Thread t5 = new Thread(tr5);
        // Thread t6 = new Thread(tr6);
        // Thread t7 = new Thread(tr7);
        // Thread t8 = new Thread(tr8);
        // Thread t9 = new Thread(tr9);
        t.start();
        // t1.start();
        // t2.start();
        // t3.start();
        // t4.start();
        // t5.start();
        // t6.start();
        // t7.start();
        // t8.start();
        // t9.start();
        t.join();
        // t1.join();
        // t2.join();
        // t3.join();
        // t4.join();
        // t5.join();
        // t6.join();
        // t7.join();
        // t8.join();
        // t9.join();
        System.out.println("Time Consumed: " + (System.currentTimeMillis() - startTime)/1000);
    }

}
