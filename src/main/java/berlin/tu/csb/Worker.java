package berlin.tu.csb;

import org.apache.commons.lang3.RandomUtils;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Worker {
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Incorrect number of arguments: " + args.length + "/2");
            System.out.println("1 - ip address of SUT");
            System.out.println("2 - start time");
        }
        if (args.length == 0) {
            System.out.println("No Argument found. Try to get ip from local file.");
            args = new String[1];
            System.out.println("Present Project Directory : " + System.getProperty("user.dir"));


            String content = "";
            try {
                content = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "\\terraform\\public_ip.csv")));
            } catch (IOException e) {
                System.out.println("public_ip.csv not present. Are the servers set up correctly?");
                e.printStackTrace();
                System.exit(1);
            }

            String[] serverAddresses = content.split(",");
            args[0] = serverAddresses[0];
        }

        System.out.println("Try connecting to database " + args[0]);

        // Configure the database connection.
        //PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{args[0]});
        ds.setPortNumbers(new int[]{26257});
        ds.setDatabaseName("tpc_w_light");
        ds.setUser("root");
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("woker-" + args[0]);
        //ds.setConnectTimeout();

        // Create DAO.
        BenchmarkDAO dao = new BenchmarkDAO(ds);




        long t1_1 = System.currentTimeMillis();
        long startTime = System.currentTimeMillis() + 5000;
        long runTimeInSeconds = 5;
        long endTime = startTime + runTimeInSeconds * 1000;

        WorkloadGenerator workloadGenerator = new WorkloadGenerator(dao,startTime,runTimeInSeconds,endTime);


        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
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






        /*
        // Print out 10 account values.
        List<Customer> randomCustomers = dao.getRandomCustomers(10);

        for (Customer customer : randomCustomers) {
            System.out.println(customer);
        }

        // Print out 10 account values.
        List<Item> randomItems = dao.getRandomItems(10);

        for (Item item : randomItems) {
            System.out.println(item);
        }


        int totalRowsInserted = dao.bulkInsertRandomOrders(500, randomCustomers);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomOrders:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        // Print out 10 account values.
        List<Orders> randomOrders = dao.getRandomOrders(10);

        for (Orders orders : randomOrders) {
            System.out.println(orders);
        }

        // Print out 10 account values.
        Customer customer = randomCustomers.get(RandomUtils.nextInt(0, randomCustomers.size()));
        List<Orders> ordersForCustomer = dao.getOrdersFromCustomer(customer.c_id);
        System.out.println("All Orders for customer " + customer.c_id);
        for (Orders orders : ordersForCustomer) {
            System.out.println(orders);
        }

        totalRowsInserted = dao.bulkInsertRandomOrderLine(500, randomOrders, randomItems);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomOrderLine:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        Orders order = randomOrders.get(RandomUtils.nextInt(0, randomOrders.size()));
        List<OrderLine> orderLinesForOrder = dao.getOrderLinesFromOrder(order.o_id);
        System.out.println("All OrderLines for order " + order.o_id);
        for (OrderLine orderLine : orderLinesForOrder) {
            System.out.println(orderLine);
        }


         */
        System.out.println(dao.sqlLog);
        System.out.println("Log size:" + dao.sqlLog.size());
        System.out.println("Executed " + dao.sqlLog.size() + " in " + (t1_2-t1_1)/1000 + " seconds. " + dao.sqlLog.size() / runTimeInSeconds + "t/s AVG");

    }
}
