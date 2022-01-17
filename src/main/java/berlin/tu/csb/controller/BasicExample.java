package berlin.tu.csb.controller;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import org.apache.commons.lang3.RandomUtils;
import org.postgresql.ds.PGSimpleDataSource;


/**
 * Main class for the basic JDBC example.
 **/
public class BasicExample {

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

        System.out.println("Connecting to databases " + serverAddresses );

        // Configure the database connection.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{serverAddresses[0]});
        ds.setPortNumbers(new int[]{26257});
        ds.setDatabaseName("tpc_w_light");
        ds.setUser("root");
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("BasicExample");

        PGSimpleDataSource ds2 = new PGSimpleDataSource();
        ds2.setServerNames(new String[]{serverAddresses[1]});
        ds2.setPortNumbers(new int[]{26257});
        ds2.setDatabaseName("tpc_w_light");
        ds2.setUser("root");
        ds2.setSsl(false);
        ds2.setSslMode("disable");
        ds2.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds2.setApplicationName("BasicExample");

        // Create DAO.
        BenchmarkDAO dao = new BenchmarkDAO(ds);
        BenchmarkDAO dao2 = new BenchmarkDAO(ds2);

        // Test our retry handling logic if FORCE_RETRY is true.  This
        // method is only used to test the retry logic.  It is not
        // necessary in production code.
        dao.testRetryHandling();

        dao.truncateAllTables();

        int totalRowsInserted = dao.bulkInsertRandomCustomerData(500);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomCustomerData:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        totalRowsInserted = dao.bulkInsertRandomItemData(500);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomItemData:\n    => finished, %s total rows inserted\n", totalRowsInserted);



        // Print out 10 account values.
        List<Customer> randomCustomers = dao2.getRandomCustomers(10);

        for (Customer customer: randomCustomers) {
            System.out.println(customer);
        }

        // Print out 10 account values.
        List<Item> randomItems = dao2.getRandomItems(10);

        for (Item item: randomItems) {
            System.out.println(item);
        }


        totalRowsInserted = dao.bulkInsertRandomOrders(500, randomCustomers);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomOrders:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        // Print out 10 account values.
        List<Order> randomOrders = dao2.getRandomOrders(10);

        for (Order order : randomOrders) {
            System.out.println(order);
        }

        // Print out 10 account values.
        Customer customer = randomCustomers.get(RandomUtils.nextInt(0, randomCustomers.size()));
        List<Order> orderForCustomer = dao2.getOrdersFromCustomer(customer.c_id);
        System.out.println("All Orders for customer " + customer.c_id);
        for (Order order : orderForCustomer) {
            System.out.println(order);
        }

        totalRowsInserted = dao.bulkInsertRandomOrderLine(500, randomOrders, randomItems);
        System.out.printf("\nBenchmarkDAO. bulkInsertRandomOrderLine:\n    => finished, %s total rows inserted\n", totalRowsInserted);

        Order order = randomOrders.get(RandomUtils.nextInt(0, randomOrders.size()));
        List<OrderLine> orderLinesForOrder = dao2.getOrderLinesFromOrder(order.o_id);
        System.out.println("All OrderLines for order " + order.o_id);
        for (OrderLine orderLine: orderLinesForOrder) {
            System.out.println(orderLine);
        }

        System.out.println("Insert Log size:" + dao.sqlLog.size());
        System.out.println("Select Log size:" + dao2.sqlLog.size());



    }
}

