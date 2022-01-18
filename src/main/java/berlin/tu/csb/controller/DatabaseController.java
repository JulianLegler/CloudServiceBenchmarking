package berlin.tu.csb.controller;


import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.List;

public class DatabaseController {
    BenchmarkDAO dao;


    public DatabaseController(String dbName, String dbUserName, int dbPort, String[] serverAddresses) {

        System.out.println("Connecting to databases " + serverAddresses[0] );

        // Configure the database connection.

        /* Default JDBC for postgres - Without connection pooling


        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{serverAddresses[0]});
        ds.setPortNumbers(new int[]{dbPort});
        ds.setDatabaseName(dbName);
        ds.setUser(dbUserName);
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("BasicExample");
        */

        /* Apache Commons DBCP - With Connection Pooling


        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(String.format("jdbc:postgresql://%s:%s/%s", serverAddresses[0], dbPort, dbName));
        ds.setUsername(dbUserName);
        */

        /* HikariCP - With connection pooling
         */
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", serverAddresses[0], dbPort, dbName));
        config.setUsername("root");

        HikariDataSource ds = new HikariDataSource(config);



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

    public boolean insertOrder(Order order) {
        if(dao.insertOrderIntoDB(order)) {
            return true;
        }
        else
            return false;
    }

    public boolean insertOrderLines(List<OrderLine> orderLineList) {
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
