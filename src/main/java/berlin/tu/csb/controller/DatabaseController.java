package berlin.tu.csb.controller;


import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.List;

public class DatabaseController {
    BenchmarkDAO dao;

    public DatabaseController(String dbName, String dbUserName, int dbPort, String[] serverAddresses) {

        System.out.println("Connecting to databases " + serverAddresses[0] );

        // Configure the database connection.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{serverAddresses[0]});
        ds.setPortNumbers(new int[]{dbPort});
        ds.setDatabaseName(dbName);
        ds.setUser(dbUserName);
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("BasicExample");


        // Create DAO
        this.dao = new BenchmarkDAO(ds);
    }

    public boolean insertCustomer(Customer customer) {
        if(dao.insertCustomerIntoDB(customer)) {
            return true;
        }
        else
            return false;
    }

    public boolean insertItem(Item item) {
        if(dao.insertItemIntoDB(item)) {
            return true;
        }
        else
            return false;
    }

    private boolean insertOrder(Order order) {
        if(dao.insertOrderIntoDB(order)) {
            return true;
        }
        else
            return false;
    }

    private boolean insertOrderLines(List<OrderLine> orderLineList) {
        if(dao.insertOrderLinesIntoDB(orderLineList)) {
            return true;
        }
        else
            return false;
    }

    public boolean insertOrderWithOrderLines(Order order, List<OrderLine> orderLines) {
        if(insertOrder(order) && insertOrderLines(orderLines)) {
            return true;
        }
        else
            return false;
    }

    // TODO: some getters

}
