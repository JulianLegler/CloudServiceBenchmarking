package berlin.tu.csb.controller;

import berlin.tu.csb.model.BenchmarkConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

public class MainController {
    static DatabaseController databaseController;
    static int threadCount = 5;
    // Create for each iteration a new directory with the creation timestamp of the workload
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
    static Date now = new Date(System.currentTimeMillis());
    static String dateString = sdf.format(now);
    static Logger logger = LogManager.getLogger(MainController.class);


    public static void main(String[] args) {
        // default values
        String[] serverAddresses = new String[1];
        long seed = 2122;
        int runTimeInMinutes = 1;
        int threadCount = 10;
        String runOrLoad = "run";
        int dbCustomerInsertsLoadPhase = 100000;
        int dbItemInsertsLoadPhase = 2000 ;

        System.out.println("Present Project Directory : "+ System.getProperty("user.dir"));


        if(args[0] != null && args[0].equals("run")) {
            if(args.length < 4 || args.length > 5) {
                System.out.println("Number of run arguments are not correct. Fallback to local execution mode.");
                System.out.printf("Correct usage of parameters:%n 1 - run %n 2 - server adress%n 3 - seed for pseudo generator as long%n 4 - run time of benchmark in minutes%n 5 - amount of threads to run%n");
                try {
                    String content = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir")+"\\terraform\\public_ip.csv")));
                    serverAddresses = content.split(",");
                } catch (IOException e) {
                    System.out.println("public_ip.csv not present. Are the servers set up correctly?");
                    e.printStackTrace();
                    System.exit(1);
                }
            }else {
                serverAddresses[0] = args[1];
                seed = Long.parseLong(args[2]);
                runTimeInMinutes = Integer.parseInt(args[3]);
                threadCount = Integer.parseInt(args[4]);
            }
        }
        else if (args[0] != null && args[0].equals("load")) {
            runOrLoad = "load";
            if(args.length < 4 || args.length > 5) {
                System.out.println("Number of run arguments are not correct. Fallback to local execution mode.");
                System.out.printf("Correct usage of parameters:%n 1 - load %n 2 - server adress%n 3 - seed for pseudo generator as long%n 4 - Amount of Customers to be created %n 5 - Amount of Items to be created %n 6 - amount of threads to run%n");
                try {
                    String content = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "\\terraform\\public_ip.csv")));
                    serverAddresses = content.split(",");
                } catch (IOException e) {
                    System.out.println("public_ip.csv not present. Are the servers set up correctly?");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else {
                serverAddresses[0] = args[1];
                seed = Long.parseLong(args[2]);
                dbCustomerInsertsLoadPhase = Integer.parseInt(args[3]);
                dbItemInsertsLoadPhase = Integer.parseInt(args[4]);
                threadCount = Integer.parseInt(args[5]);
            }
        }






        BenchmarkConfig benchmarkConfig = new BenchmarkConfig();
        benchmarkConfig.dbCustomerInsertsLoadPhase = dbCustomerInsertsLoadPhase;
        benchmarkConfig.dbItemInsertsLoadPhase = dbItemInsertsLoadPhase ;
        benchmarkConfig.dbOrderInsertsLoadPhase = (long)(benchmarkConfig.dbCustomerInsertsLoadPhase * 1.2);
        benchmarkConfig.threadCountLoad = threadCount;
        benchmarkConfig.threadCountRun = threadCount;
        benchmarkConfig.seed = seed;
        benchmarkConfig.minRunTimeOfRunPhaseInMinutes = runTimeInMinutes;
        benchmarkConfig.initialWaitTimeForCoordinationInSeconds = 5;
        benchmarkConfig.useCasesProbabilityDistribution = new LinkedHashMap<>();
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchRandomTopSellerItem", 35);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchRandomItem", 20);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchRandomCustomer", 10);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchOrdersFromRandomCustomer", 5);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchOrderLinesFromRandomOrder", 5);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchItemsSortedByName", 5);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchItemsSortedByPrice", 5);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchItemsWithStringInName", 5);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$insertNewOrder", 6);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$insertNewCustomer", 1);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$fetchAllCustomersWithOpenOrders", 1);
        benchmarkConfig.useCasesProbabilityDistribution.put("berlin.tu.csb.controller.RunPhaseGenerator$updateItemPrice", 2);




        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String json = gson.toJson(benchmarkConfig);
        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", dateString, "config.json");
        try {
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, json, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            e.printStackTrace();
        }

        benchmarkConfig = null;


        String readedJson = null;
        try {
            readedJson = Files.readString(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        benchmarkConfig = gson.fromJson(readedJson, BenchmarkConfig.class);








        // String pickedServerAddress = serverAddresses[ThreadLocalRandom.current().nextInt(0, serverAddresses.length)];

        databaseController = new DatabaseController("tpc_w_light", "root", 26257, serverAddresses[0]);

        //databaseController.dao.truncateAllTables();


        if(runOrLoad.equals("load")) {
            runLoadPhase(serverAddresses, benchmarkConfig);
        }
        else {
            runRunPhase(serverAddresses, benchmarkConfig);
        }



    }

    private static void runRunPhase(String[] serverAddresses, BenchmarkConfig benchmarkConfig) {
        long t1_1 = System.currentTimeMillis();
        long startTime = System.currentTimeMillis() + benchmarkConfig.initialWaitTimeForCoordinationInSeconds * 1000;
        long runTimeInSeconds = 60 * benchmarkConfig.minRunTimeOfRunPhaseInMinutes;
        long endTime = startTime + runTimeInSeconds * 1000;

        List<Thread> threadList = new ArrayList<>();
        List<PersistenceController> persistenceControllerList = new ArrayList<>();

        SeededRandomHelper seededRandomHelperPre = new SeededRandomHelper(benchmarkConfig.seed);
        PersistenceController persistenceControllerPre = new PersistenceController(new DatabaseController("tpc_w_light", "root", 26257, serverAddresses[0]), new StateController(seededRandomHelperPre));

        logger.info("Fetching Customers from DB...");
        persistenceControllerPre.syncCustomerStateWithDB();
        logger.info("Fetched " + persistenceControllerPre.stateController.getCustomerListSize() + " Customer from DB.");

        logger.info("Fetching Items from DB...");
        persistenceControllerPre.syncItemStateWithDB();
        logger.info("Fetched " + persistenceControllerPre.stateController.getItemListSize() + " Items from DB.");

        logger.info("Fetching Orders from DB...");
        persistenceControllerPre.syncOrderStateWithDB();
        logger.info("Fetched " + persistenceControllerPre.stateController.getOrderSize() + " Orders from DB.");

        logger.info("Fetching OrderLines from DB...");
        persistenceControllerPre.syncOrderLineStateWithDB();
        logger.info("Fetched " + persistenceControllerPre.stateController.getOrderLineSize() + " OrderLines from DB.");


        for (int i = 1; i <= benchmarkConfig.threadCountRun; i++) {
            String pickedServerAddress = serverAddresses[i % serverAddresses.length];

            // Have a PersistenceController per thread to manage the current database part that is used by this thread so they dont interfere with each other
            SeededRandomHelper seededRandomHelper = new SeededRandomHelper(benchmarkConfig.seed+i);
            PersistenceController persistenceController = new PersistenceController(new DatabaseController("tpc_w_light", "root", 26257, pickedServerAddress), new StateController(seededRandomHelper));
            persistenceControllerList.add(persistenceController);
            persistenceController.syncPersistenceController(persistenceControllerPre);

            RunPhaseGenerator runPhaseGenerator = new RunPhaseGenerator(persistenceController, seededRandomHelper, startTime, runTimeInSeconds, endTime, benchmarkConfig);
            //workloadGenerator.run();

            Thread thread = new Thread(runPhaseGenerator);
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

        int sqlCounter = 0;
        for (PersistenceController persistenceController : persistenceControllerList) {
            //System.out.println("Thread-" + counter + ": " + persistenceController.databaseController.dao.sqlLog);
            //counter++;
            sqlCounter += persistenceController.databaseController.dao.sqlLog.size();
        }




        //System.out.println(databaseController.dao.sqlLog);
        System.out.println("Log size:" + sqlCounter);
        System.out.println("Executed " + sqlCounter + " in " + (t1_2-t1_1)/1000 + " seconds. " + sqlCounter / runTimeInSeconds + "t/s AVG of planned time and " + sqlCounter / ((t1_2-t1_1)/1000) + " t/s AVG on the actual time used");


        // use GSON to create json objects of the safed workload queries and safe them to a directory
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        for (PersistenceController persistenceController: persistenceControllerList) {

            System.out.println("Customers in DB:" + persistenceController.stateController.getCustomerListSize());
            System.out.println("Items in DB:" + persistenceController.stateController.getItemListSize());
            System.out.println("Orders in DB:" + persistenceController.stateController.getOrderSize());

            String json = gson.toJson(persistenceController.databaseController.workloadQueryController.workloadQueryList);
            //System.out.println(json);

            long threadId = persistenceController.databaseController.workloadQueryController.workloadContextId;

            Path filePath = Paths.get(System.getProperty("user.dir"), "workload", dateString, "run_" + String.valueOf(threadId)+".json");
            try {
                Files.createDirectories(filePath.getParent());
                Files.writeString(filePath, json, StandardOpenOption.CREATE_NEW);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




    private static void runLoadPhase(String[] serverAddresses, BenchmarkConfig benchmarkConfig) {
        long t1_1 = System.currentTimeMillis();
        long startTime = System.currentTimeMillis() + 5000;
        long runTimeInSeconds = 60 * 1;
        long endTime = startTime + runTimeInSeconds * 1000;


        List<Thread> threadList = new ArrayList<>();
        List<PersistenceController> persistenceControllerList = new ArrayList<>();

        for (int i = 1; i <= benchmarkConfig.threadCountLoad; i++) {
            String pickedServerAddress = serverAddresses[i % serverAddresses.length];

            // Have a PersistenceController per thread to manage the current database part that is used by this thread so they dont interfere with each other
            SeededRandomHelper seededRandomHelper = new SeededRandomHelper(benchmarkConfig.seed-i);
            PersistenceController persistenceController = new PersistenceController(new DatabaseController("tpc_w_light", "root", 26257, pickedServerAddress), new StateController(seededRandomHelper));
            persistenceControllerList.add(persistenceController);
            LoadPhaseGenerator loadPhaseGenerator = new LoadPhaseGenerator(persistenceController, seededRandomHelper, benchmarkConfig);
            //workloadGenerator.run();

            Thread thread = new Thread(loadPhaseGenerator);
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

        int sqlCounter = 0;
        for (PersistenceController persistenceController : persistenceControllerList) {
            //System.out.println("Thread-" + counter + ": " + persistenceController.databaseController.dao.sqlLog);
            //counter++;
            sqlCounter += persistenceController.databaseController.dao.sqlLog.size();
        }




        //System.out.println(databaseController.dao.sqlLog);
        System.out.println("Log size:" + sqlCounter);
        System.out.println("Executed " + sqlCounter + " in " + (t1_2-t1_1)/1000 + " seconds. ");


        // use GSON to create json objects of the safed workload queries and safe them to a directory
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        for (PersistenceController persistenceController: persistenceControllerList) {

            System.out.println("Customers in DB:" + persistenceController.stateController.getCustomerListSize());
            System.out.println("Items in DB:" + persistenceController.stateController.getItemListSize());
            System.out.println("Orders in DB:" + persistenceController.stateController.getOrderSize());

            String json = gson.toJson(persistenceController.databaseController.workloadQueryController.workloadQueryList);
            //System.out.println(json);

            long threadId = persistenceController.databaseController.workloadQueryController.workloadContextId;

            Path filePath = Paths.get(System.getProperty("user.dir"), "workload", dateString, "load_" + String.valueOf(threadId)+".json");
            try {
                Files.createDirectories(filePath.getParent());
                Files.writeString(filePath, json, StandardOpenOption.CREATE_NEW);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
