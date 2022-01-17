package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MainController {
    static DatabaseController databaseController;

    public static void main(String[] args) {
        System.out.println("Present Project Directory : "+ System.getProperty("user.dir"));


        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir")+"\\terraform\\public_ip.csv")));
        } catch (IOException e) {
            System.out.println("public_ip.csv not present. Are the servers set up correctly?");
            e.printStackTrace();
            System.exit(1);
        }

        String[] serverAddresses = content.split(",");

        databaseController = new DatabaseController("tpc_w_light", "root", 26257, serverAddresses);


        long t1_1 = System.currentTimeMillis();
        long startTime = System.currentTimeMillis() + 5000;
        long runTimeInSeconds = 5;
        long endTime = startTime + runTimeInSeconds * 1000;


        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            WorkloadGenerator workloadGenerator = new WorkloadGenerator(new StateController(), databaseController, startTime, runTimeInSeconds, endTime);
            Thread thread = new Thread(workloadGenerator);
            threadList.add(thread);
            thread.start();
        }

        try {
            for (Thread thread : threadList) {
                thread.join();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long t1_2 = System.currentTimeMillis();


        System.out.println(databaseController.dao.sqlLog);
        System.out.println("Log size:" + databaseController.dao.sqlLog.size());
        System.out.println("Executed " + databaseController.dao.sqlLog.size() + " in " + (t1_2-t1_1)/1000 + " seconds. " + databaseController.dao.sqlLog.size() / runTimeInSeconds + "t/s AVG");

    }

}
