package berlin.tu.csb.controller;

import berlin.tu.csb.model.*;
import com.zaxxer.hikari.pool.HikariProxyPreparedStatement;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Data access object used by 'BasicExample'.  Abstraction over some
 * common CockroachDB operations, including:
 * <p>
 * - Auto-handling transaction retries in the 'runSQL' method
 * <p>
 * - Example of bulk inserts in the 'bulkInsertRandomAccountData'
 * method
 */

class BenchmarkDAO {

    private static final int MAX_RETRY_COUNT = 3;
    private static final String RETRY_SQL_STATE = "40001";
    private static final boolean FORCE_RETRY = false;
    private static final int batchSize = 100;

    SeededRandomHelper seededRandomHelper = new SeededRandomHelper();

    public List<String> sqlLog;
    private WorkloadQueryController workloadQueryController;

    public static Logger logger = LogManager.getLogger(BenchmarkDAO.class);

    private final DataSource ds;

    private final Random rand = new Random();

    BenchmarkDAO(DataSource ds, WorkloadQueryController workloadQueryController) {
        this.ds = ds;
        this.workloadQueryController = workloadQueryController;
        this.sqlLog = new ArrayList<>();
    }

    /**
     * Used to test the retry logic in 'runSQL'.  It is not necessary
     * in production code.
     */
    void testRetryHandling() {
        if (BenchmarkDAO.FORCE_RETRY) {
            runSQL("SELECT crdb_internal.force_retry('1s':::INTERVAL)");
        }
    }

    /**
     * Run SQL code in a way that automatically handles the
     * transaction retry logic so we don't have to duplicate it in
     * various places.
     *
     * @param sqlCode a String containing the SQL code you want to
     *                execute.  Can have placeholders, e.g., "INSERT INTO accounts
     *                (id, balance) VALUES (?, ?)".
     * @param args    String Varargs to fill in the SQL code's
     *                placeholders.
     * @return Integer Number of rows updated, or -1 if an error is thrown.
     */
    public Integer runSQL(String sqlCode, String... args) {

        // This block is only used to emit class and method names in
        // the program output.  It is not necessary in production
        // code.
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement elem = stacktrace[2];
        String callerClass = elem.getClassName();
        String callerMethod = elem.getMethodName();

        int rv = 0;

        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // automatically issue transaction retries.
            connection.setAutoCommit(false);

            int retryCount = 0;

            while (retryCount <= MAX_RETRY_COUNT) {

                if (retryCount == MAX_RETRY_COUNT) {
                    String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
                    throw new RuntimeException(err);
                }

                // This block is only used to test the retry logic.
                // It is not necessary in production code.  See also
                // the method 'testRetryHandling()'.
                if (FORCE_RETRY) {
                    forceRetry(connection); // SELECT 1
                }

                try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

                    // Loop over the args and insert them into the
                    // prepared statement based on their types.  In
                    // this simple example we classify the argument
                    // types as "integers" and "everything else"
                    // (a.k.a. strings).
                    for (int i = 0; i < args.length; i++) {
                        int place = i + 1;
                        String arg = args[i];

                        try {
                            int val = Integer.parseInt(arg);
                            pstmt.setInt(place, val);
                        } catch (NumberFormatException e) {
                            pstmt.setString(place, arg);
                        }
                    }

                    if (pstmt.execute()) {
                        // We know that `pstmt.getResultSet()` will
                        // not return `null` if `pstmt.execute()` was
                        // true
                        ResultSet rs = pstmt.getResultSet();
                        ResultSetMetaData rsmeta = rs.getMetaData();
                        int colCount = rsmeta.getColumnCount();

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);

                        while (rs.next()) {
                            for (int i = 1; i <= colCount; i++) {
                                String name = rsmeta.getColumnName(i);
                                String type = rsmeta.getColumnTypeName(i);

                                // In this "bank account" example we know we are only handling
                                // integer values (technically 64-bit INT8s, the CockroachDB
                                // default).  This code could be made into a switch statement
                                // to handle the various SQL types needed by the application.
                                if ("int8".equals(type)) {
                                    int val = rs.getInt(name);

                                    // This printed output is for debugging and/or demonstration
                                    // purposes only.  It would not be necessary in production code.
                                    System.out.printf("    %-8s => %10s\n", name, val);
                                }
                            }
                        }
                    } else {
                        int updateCount = pstmt.getUpdateCount();
                        rv += updateCount;

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);
                    }

                    connection.commit();
                    break;

                } catch (SQLException e) {

                    if (RETRY_SQL_STATE.equals(e.getSQLState())) {
                        // Since this is a transaction retry error, we
                        // roll back the transaction and sleep a
                        // little before trying again.  Each time
                        // through the loop we sleep for a little
                        // longer than the last time
                        // (A.K.A. exponential backoff).
                        System.out.printf("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount);
                        connection.rollback();
                        retryCount++;
                        int sleepMillis = (int) (Math.pow(2, retryCount) * 100) + rand.nextInt(100);
                        System.out.printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {
                            // Necessary to allow the Thread.sleep()
                            // above so the retry loop can continue.
                        }

                        rv = -1;
                    } else {
                        rv = -1;
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.runSQL ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
            rv = -1;
        }

        return rv;
    }

    public boolean insertSingleObjectToDB(DatabaseTableModel databaseTableModel) {
        try (Connection connection = ds.getConnection()) {
            try (PreparedStatement pstmt = connection.prepareStatement(databaseTableModel.getSQLInsertString())) {

                databaseTableModel.fillStatement(pstmt);
                sqlLog.add(pstmt.toString());
                workloadQueryController.add(pstmt.toString());
                logger.trace(pstmt.toString());

                pstmt.execute();
                pstmt.close();
                connection.close();
            } catch (SQLException e) {
                System.out.printf("BenchmarkDAO.insertSingleObjectIntoDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                        databaseTableModel.getClass(), e.getSQLState(), e.getCause(), e.getMessage());
                return false;
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.insertSingleObjectIntoDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                    databaseTableModel.getClass(), e.getSQLState(), e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }

    public boolean bulkInsertObjectsToDB(List<? extends DatabaseTableModel> databaseTableModelList) {
        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // control the size of our batch inserts.
            connection.setAutoCommit(false);

            try (PreparedStatement pstmt = connection.prepareStatement(databaseTableModelList.get(0).getSQLInsertString())) {
                int counter = 0;
                for (DatabaseTableModel databaseTableModel : databaseTableModelList) {
                    counter++;
                    databaseTableModel.fillStatement(pstmt);
                    sqlLog.add(pstmt.toString());
                    workloadQueryController.add(pstmt.toString());
                    logger.trace(pstmt.toString());
                    pstmt.addBatch();
                    if (counter % batchSize == 0 || counter == databaseTableModelList.size()) {
                        int[] count = pstmt.executeBatch();
                        logger.trace(String.format("\nBenchmarkDAO.bulkInsertObjectsToDB:\n    '%s'\n", pstmt));
                        logger.trace(String.format("    => %s row(s) updated in this batch\n", count.length));
                    }
                }
                connection.commit();
                pstmt.close();
                connection.close();
            } catch (SQLException e) {
                logger.error(String.format("BenchmarkDAO.bulkInsertObjectsToDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                        databaseTableModelList.get(0).getClass(),e.getSQLState(), e.getCause(), e.getMessage()));
                return false;
            }
        } catch (SQLException e) {
            logger.error(String.format("BenchmarkDAO.bulkInsertObjectsToDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    databaseTableModelList.get(0).getClass(), e.getSQLState(), e.getCause(), e.getMessage()));
            return false;
        }
        return true;
    }

    /**
     * Helper method called by 'testRetryHandling'.  It simply issues
     * a "SELECT 1" inside the transaction to force a retry.  This is
     * necessary to take the connection's session out of the AutoRetry
     * state, since otherwise the other statements in the session will
     * be retried automatically, and the client (us) will not see a
     * retry error. Note that this information is taken from the
     * following test:
     * https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/logictest/testdata/logic_test/manual_retry
     *
     * @param connection Connection
     */
    private void forceRetry(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
            statement.executeQuery();
        }
    }

    public boolean bulkInsertCustomersToDB(List<Customer> customerList) {
        return bulkInsertObjectsToDB(customerList);
    }

    public boolean insertCustomerIntoDB(Customer customer) {
        return insertSingleObjectToDB(customer);
    }

    public List<Customer> getRandomCustomers(int limit) {
        ArrayList<Customer> randomCustomers = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM customer ORDER BY random() LIMIT %d;", limit);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {
                    Customer customer = new Customer();
                    customer.initWithResultSet(rs);
                    randomCustomers.add(customer);
                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getRandomCostumers ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return randomCustomers;
    }

    public Customer getCustomerFromDB(Customer customer) {
        Customer updatedCustomer = null;
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM customer WHERE c_id = '%s';", customer.c_id);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {
                    updatedCustomer = new Customer();
                    updatedCustomer.initWithResultSet(rs);
                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getCustomerFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return updatedCustomer;
    }


    public boolean insertItemIntoDB(Item item) {
        return insertSingleObjectToDB(item);
    }


    public List<Item> getRandomItems(int limit) {
        ArrayList<Item> randomItems = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM item ORDER BY random() LIMIT %d;", limit);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Item item = new Item();
                    item.initWithResultSet(rs);
                    randomItems.add(item);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getRandomCostumers ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return randomItems;
    }

    public Item getItemFromDB(Item item) {
        Item itemFromDB = null;
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM item WHERE i_id = '%s'", item.i_id);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    itemFromDB = new Item();
                    itemFromDB.initWithResultSet(rs);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getRandomCostumers ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return itemFromDB;
    }

    public boolean bulkInsertItemsToDB(List<Item> itemList) {
        return bulkInsertObjectsToDB(itemList);
    }

    public boolean bulkInsertOrdersToDB(List<Order> orderList) {
        return bulkInsertObjectsToDB(orderList);
    }


    public boolean insertOrderIntoDB(Order order) {
        return insertSingleObjectToDB(order);
    }

    public List<Order> getRandomOrders(int limit) {
        ArrayList<Order> randomOrders = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM orders ORDER BY random() LIMIT %d;", limit);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Order order = new Order();
                    order.initWithResultSet(rs);
                    randomOrders.add(order);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getRandomOrders ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return randomOrders;
    }

    public List<Order> getOrdersFromCustomer(String customerID) {
        ArrayList<Order> randomOrders = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM orders WHERE c_id = '%s'", customerID);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Order order = new Order();
                    order.initWithResultSet(rs);
                    randomOrders.add(order);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getRandomOrders ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return randomOrders;
    }

    public List<Order> getOrdersOfCustomerFromDB(Customer customer) {
        ArrayList<Order> customerOrders = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM orders WHERE c_id = '%s'", customer.c_id);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Order order = new Order();
                    order.initWithResultSet(rs);
                    customerOrders.add(order);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getOrdersOfCustomerFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return customerOrders;
    }

    public Order getOrderOfCustomerFromDB(Order order) {
        Order updatedOrder = null;
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM orders WHERE o_id = '%s'", order.o_id);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    updatedOrder = new Order();
                    updatedOrder.initWithResultSet(rs);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getOrderOfCustomerFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return updatedOrder;
    }

    public boolean insertOrderLinesIntoDB(List<OrderLine> orderLineList) {
        return bulkInsertObjectsToDB(orderLineList);
    }

    public List<OrderLine> getOrderLinesFromOrder(String orderID) {
        ArrayList<OrderLine> orderLines = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM order_line WHERE o_id = '%s'", orderID);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    OrderLine orderLine = new OrderLine();
                    orderLine.initWithResultSet(rs);
                    orderLines.add(orderLine);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getOrderLinesFromOrder ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return orderLines;
    }

    public List<OrderLine> getOrderLinesOfOrderFromDB(Order order) {
        ArrayList<OrderLine> orderLines = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM order_line WHERE o_id = '%s'", order.o_id);
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    OrderLine orderLine = new OrderLine();
                    orderLine.initWithResultSet(rs);
                    orderLines.add(orderLine);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getOrderLinesOfOrderFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return orderLines;
    }


    public void truncateAllTables() {
        logger.warn("All tables truncated!");
        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // control the size of our batch inserts.
            //connection.setAutoCommit(false);

            // In this example we are adding 500 rows to the database,
            // but it could be any number.  What's important is that
            // the batch size is 128.
            try (PreparedStatement pstmt = connection.prepareStatement("TRUNCATE TABLE customer CASCADE; TRUNCATE TABLE orders CASCADE; TRUNCATE TABLE item CASCADE; TRUNCATE TABLE order_line CASCADE;")) {

                sqlLog.add(pstmt.toString());
                workloadQueryController.add(pstmt.toString());
                logger.trace(pstmt.toString());

                pstmt.execute();
                //connection.commit();
                pstmt.close();
                connection.close();
            } catch (SQLException e) {
                System.out.printf("BenchmarkDAO.insertRandomCustomer ERROR: { state => %s, cause => %s, message => %s }\n",
                        e.getSQLState(), e.getCause(), e.getMessage());
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.insertRandomCustomer ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
    }

    public List<Item> getItemsFromDB(List<Item> itemList) {
        ArrayList<Item> updatedItemList = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM item WHERE i_id IN ('%s')", itemList.stream().map(i -> i.i_id).collect(Collectors.joining("','")));
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Item item = new Item();
                    item.initWithResultSet(rs);
                    updatedItemList.add(item);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getItemsFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return updatedItemList;
    }


    public List<Customer> getAllCustomersFromDB() {

        List<Customer> customerList = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM customer");
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {
                    Customer customer = new Customer();
                    customer.initWithResultSet(rs);
                    customerList.add(customer);
                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getAllCustomersFromDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return customerList;
    }

    public List<Item> getAllItemsFormDB() {
        ArrayList<Item> itemList = new ArrayList<>();
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("SELECT * FROM item");
                sqlLog.add(sqlStatement);
                workloadQueryController.add(sqlStatement);
                logger.trace(sqlStatement);
                ResultSet rs = statement.executeQuery(sqlStatement);

                while (rs.next()) {

                    Item item = new Item();
                    item.initWithResultSet(rs);
                    itemList.add(item);

                }

                rs.close();

                statement.close();

                connection.close();

            } catch (Exception e) {

                System.err.println(e.getClass().getName() + ": " + e.getMessage());

                System.exit(0);

            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getAllItemsFormDB ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
        return itemList;
    }


}
