package berlin.tu.csb.controller;


import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DatabaseController {
    BenchmarkDAO dao;
    WorkloadQueryController workloadQueryController;


    public DatabaseController(String dbName, String dbUserName, int dbPort, String serverAddress) {




        System.out.println("Connecting to databases " + serverAddress);

        // Configure the database connection.

        /* Default JDBC for postgres - Without connection pooling

         */
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{serverAddress});
        ds.setPortNumbers(new int[]{dbPort});
        ds.setDatabaseName(dbName);
        ds.setUser(dbUserName);
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("BasicExample");


        /* Apache Commons DBCP - With Connection Pooling


        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(String.format("jdbc:postgresql://%s:%s/%s", serverAddresses[0], dbPort, dbName));
        ds.setUsername(dbUserName);
        */

        /* HikariCP - With connection pooling

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", serverAddresses[0], dbPort, dbName));
        config.setUsername("root");

        HikariDataSource ds = new HikariDataSource(config);
        */


        this.workloadQueryController = new WorkloadQueryController();
        // Create DAO
        this.dao = new BenchmarkDAO(ds, workloadQueryController);
    }

    public boolean insertCustomer(Customer customer) {
        return dao.insertCustomerIntoDB(customer);
    }

    public boolean bulkInsertCustomers(List<Customer> customerList) {return dao.bulkInsertCustomersToDB(customerList);}

    public boolean insertItem(Item item) {
        return dao.insertItemIntoDB(item);
    }

    public boolean bulkInsertItems(List<Item> itemList) { return dao.bulkInsertItemsToDB(itemList);}

    public boolean insertOrder(Order order) {
        return dao.insertOrderIntoDB(order);
    }

    public boolean insertOrderLines(List<OrderLine> orderLineList) {
        return dao.insertOrderLinesIntoDB(orderLineList);
    }

    public boolean insertOrderWithOrderLines(Order order, List<OrderLine> orderLines) {
        return insertOrder(order) && insertOrderLines(orderLines);
    }

    public Item getItem(Item item) {
        return dao.getItemFromDB(item);
    }

    public List<Item> getItems(List<Item> itemList) {
        return dao.getItemsFromDB(itemList);
    }

    public Customer getCustomer(Customer customer) {
        return dao.getCustomerFromDB(customer);
    }

    public Order getOrder(Order order) {
        return dao.getOrderOfCustomerFromDB(order);
    }

    public List<Order> getOrdersOfCustomer(Customer customer) {
        return dao.getOrdersOfCustomerFromDB(customer);
    }

    public List<OrderLine> getOrderLinesOfOrder(Order order) {
        return dao.getOrderLinesOfOrderFromDB(order);
    }

    public List<Customer> fetchAllCustomers() { return dao.getAllCustomersFromDB();}


    public boolean bulkInsertOrders(List<Order> orderList) { return dao.bulkInsertOrdersToDB(orderList); }

    public List<Item> fetchAllItems() { return dao.getAllItemsFormDB(); }

    public List<Order> fetchAllOrders() { return dao.getAllOrdersFromDB(); }

    public List<OrderLine> fetchAllOrderLines() { return dao.getAllOrderLinesFromDB(); }

    public List<Item> getItemsSortedByPrice(int limit) { return dao.getItemsFromDBOrderedByPrice(limit); }

    public List<Item> getItemsSortedByName(int limit) { return dao.getItemsFromDBOrderedByName(limit); }

    public List<Item> getItemsWithName(int limit, String searchName) { return dao.getItemsFromDBWhereNameContains(limit, searchName); }

    public List<Customer> getAllCustomersWithOpenOrders() { return dao.getAllCustomersWithOpenOrders(); }

    public boolean updateItemPrice(Item item) { return dao.updateItemPriceToDB(item); }
}
