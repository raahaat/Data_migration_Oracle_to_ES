package com.face.script;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mashape.unirest.http.exceptions.UnirestException;

import io.github.cdimascio.dotenv.Dotenv;

public class App {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, SQLException, UnirestException, InterruptedException {

        long startTime = System.currentTimeMillis();
        Dotenv dotenv = Dotenv.load();
        Connection con = DriverManager.getConnection(
                dotenv.get("db_url"), dotenv.get("db_user"), dotenv.get("db_password"));

        java.sql.Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(dotenv.get("db_count_query"));
        rs.next();
        int records = rs.getInt(1);
        List<Thread> threadList = new ArrayList<>();
        int threads = Integer.parseInt(dotenv.get("number_of_threads"));
        int startIndex = 0;
        int chunk = (int) Math.floor(records / threads);

        for (int i = 0; i < threads; i++) {
            if (i > 0 && (i == (threads - 1))) {
                chunk += records % threads;
            }
            ETLThread etlThread = new ETLThread(startIndex, chunk);
            startIndex += chunk;
            Thread thread = new Thread(etlThread);
            threadList.add(thread);
        }

        for (int i = 0; i < threadList.size(); i++) {
            threadList.get(i).start();
        }
        try {
            for (int i = 0; i < threadList.size(); i++) {
                threadList.get(i).join();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("-------------------------END-------------------------");
        System.out.println("Total records: " + startIndex);
        System.out.println("Sucessful: " + ETLThread.customerDataArray.size());
        System.out.println("Empty: " + ETLThread.nullData.size());
        System.out.println("Cannot Encode: " + ETLThread.cannotEncode.size());
        System.out.println("Time Consumed: " + (System.currentTimeMillis() - startTime) / 1000);
    }

}
