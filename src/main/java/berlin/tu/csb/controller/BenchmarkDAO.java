package berlin.tu.csb.controller;

import berlin.tu.csb.model.*;
import com.zaxxer.hikari.pool.HikariProxyPreparedStatement;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
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
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");

    SeededRandomHelper seededRandomHelper = new SeededRandomHelper();

    public List<String> sqlLog;
    private WorkloadQueryController workloadQueryController;

    public static Logger logger = LogManager.getLogger(BenchmarkDAO.class);

    private final DataSource ds;

    BenchmarkDAO(DataSource ds, WorkloadQueryController workloadQueryController) {
        this.ds = ds;
        this.workloadQueryController = workloadQueryController;
        this.sqlLog = new ArrayList<>();
    }

    public boolean insertSingleObjectToDB(DatabaseTableModel databaseTableModel) {
        try (Connection connection = ds.getConnection()) {
            try (PreparedStatement pstmt = connection.prepareStatement(databaseTableModel.getSQLInsertString())) {

                databaseTableModel.fillStatement(pstmt);
                sqlLog.add(pstmt.toString());
                logger.trace(pstmt.toString());


                Date now = new Date(System.currentTimeMillis());
                String timestampBeforeCommit = sdf.format(now);

                pstmt.execute();

                now = new Date(System.currentTimeMillis());
                String timestampAfterCommit = sdf.format(now);

                workloadQueryController.add(pstmt.toString(), timestampBeforeCommit, timestampAfterCommit);

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
                    workloadQueryController.add(pstmt.toString(), "", "");
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

    public DatabaseTableModel getSingleObjectFromDB(DatabaseTableModel databaseTableModel) {
        DatabaseTableModel updatedObject = null;
        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format(databaseTableModel.getBasicSQLSelfSelectString());
                sqlLog.add(sqlStatement);
                logger.trace(sqlStatement);

                Date now = new Date(System.currentTimeMillis());
                String timestampBeforeCommit = sdf.format(now);

                ResultSet rs = statement.executeQuery(sqlStatement);

                now = new Date(System.currentTimeMillis());
                String timestampAfterCommit = sdf.format(now);

                workloadQueryController.add(sqlStatement, timestampBeforeCommit, timestampAfterCommit);



                while (rs.next()) {
                    // get the implementing class of the interfaces object given to this method and create a new object of the same type and fill it afterwards
                    updatedObject = databaseTableModel.getClass().getDeclaredConstructor().newInstance();
                    updatedObject.initWithResultSet(rs);
                }
                rs.close();
                statement.close();
                connection.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getSingleObjectFromDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                    databaseTableModel.getClass(), e.getSQLState(), e.getCause(), e.getMessage());
        }
        return updatedObject;
    }

    public List<? extends DatabaseTableModel> getAllOfObjectTypeFromDB(DatabaseTableModel databaseTableModel, String additionalSQLCondition) {
        List<DatabaseTableModel> databaseTableModelList = new ArrayList<>();


        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("%s %s", databaseTableModel.getBasicSQLAllSelectString(), additionalSQLCondition);
                sqlLog.add(sqlStatement);
                logger.trace(sqlStatement);

                Date now = new Date(System.currentTimeMillis());
                String timestampBeforeCommit = sdf.format(now);

                ResultSet rs = statement.executeQuery(sqlStatement);

                now = new Date(System.currentTimeMillis());
                String timestampAfterCommit = sdf.format(now);

                workloadQueryController.add(sqlStatement, timestampBeforeCommit, timestampAfterCommit);

                while (rs.next()) {
                    // get the implementing class of the interfaces object given to this method and create a new object of the same type and fill it afterwards
                    DatabaseTableModel readDatabaseTableModel = databaseTableModel.getClass().getDeclaredConstructor().newInstance();
                    readDatabaseTableModel.initWithResultSet(rs);
                    databaseTableModelList.add(readDatabaseTableModel);
                }
                rs.close();
                statement.close();
                connection.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getAllOfObjectTypeFromDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                    databaseTableModel.getClass(), e.getSQLState(), e.getCause(), e.getMessage());
        }
        return databaseTableModelList;
    }

    public List<? extends DatabaseTableModel> getForeignObjectsByObjectPrimaryKeyFromDB(DatabaseTableModel primaryKeyTableModel, DatabaseTableModel requestedTableModel) {
        List<DatabaseTableModel> databaseTableModelList = new ArrayList<>();


        try (Connection connection = ds.getConnection()) {

            try {
                Statement statement = connection.createStatement();
                String sqlStatement = String.format("%s WHERE %s = '%s';", requestedTableModel.getBasicSQLAllSelectString(), primaryKeyTableModel.getPrimaryKeyNameAndValue().getKey(), primaryKeyTableModel.getPrimaryKeyNameAndValue().getValue());
                sqlLog.add(sqlStatement);
                logger.trace(sqlStatement);

                Date now = new Date(System.currentTimeMillis());
                String timestampBeforeCommit = sdf.format(now);

                ResultSet rs = statement.executeQuery(sqlStatement);

                now = new Date(System.currentTimeMillis());
                String timestampAfterCommit = sdf.format(now);

                workloadQueryController.add(sqlStatement, timestampBeforeCommit, timestampAfterCommit);

                while (rs.next()) {
                    // get the implementing class of the interfaces object given to this method and create a new object of the same type and fill it afterwards
                    DatabaseTableModel readDatabaseTableModel = requestedTableModel.getClass().getDeclaredConstructor().newInstance();
                    readDatabaseTableModel.initWithResultSet(rs);
                    databaseTableModelList.add(readDatabaseTableModel);
                }
                rs.close();
                statement.close();
                connection.close();
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                System.exit(0);
            }
        } catch (SQLException e) {
            System.out.printf("BenchmarkDAO.getForeignObjectsByObjectPrimaryKeyFromDB of instance %s ERROR: { state => %s, cause => %s, message => %s }\n",
                    requestedTableModel.getClass(), e.getSQLState(), e.getCause(), e.getMessage());
        }
        return databaseTableModelList;
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

    public List<? extends DatabaseTableModel> getAllOfObjectTypeFromDB(DatabaseTableModel databaseTableModel) {return getAllOfObjectTypeFromDB(databaseTableModel, "");}

    public boolean bulkInsertCustomersToDB(List<Customer> customerList) {
        return bulkInsertObjectsToDB(customerList);
    }

    public boolean insertCustomerIntoDB(Customer customer) {
        return insertSingleObjectToDB(customer);
    }

    public Customer getCustomerFromDB(Customer customer) {
        return (Customer) getSingleObjectFromDB(customer);
    }

    public boolean insertItemIntoDB(Item item) {
        return insertSingleObjectToDB(item);
    }

    public Item getItemFromDB(Item item) {
        return (Item) getSingleObjectFromDB(item);
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

    public List<Order> getOrdersOfCustomerFromDB(Customer customer) {return (List<Order>) getForeignObjectsByObjectPrimaryKeyFromDB(customer, new Order());}

    public Order getOrderOfCustomerFromDB(Order order) {
        return (Order) getSingleObjectFromDB(order);
    }

    public boolean insertOrderLinesIntoDB(List<OrderLine> orderLineList) {
        return bulkInsertObjectsToDB(orderLineList);
    }

    public OrderLine getOrderLineFromDB(OrderLine orderLine) {
        return (OrderLine) getSingleObjectFromDB(orderLine);
    }

    public List<OrderLine> getOrderLinesOfOrderFromDB(Order order) {
        return (List<OrderLine>) getForeignObjectsByObjectPrimaryKeyFromDB(order, new OrderLine());
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

                logger.trace(pstmt.toString());

                Date now = new Date(System.currentTimeMillis());
                String timestampBeforeCommit = sdf.format(now);

                pstmt.execute();

                now = new Date(System.currentTimeMillis());
                String timestampAfterCommit = sdf.format(now);

                workloadQueryController.add(pstmt.toString(), timestampBeforeCommit, timestampAfterCommit);
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

    public List<Item> getItemsFromDB(List<Item> itemList) { return (List<Item>) getAllOfObjectTypeFromDB(itemList.get(0), String.format("WHERE i_id IN ('%s')", itemList.stream().map(i -> i.i_id).collect(Collectors.joining("','")))); }

    public List<Customer> getAllCustomersFromDB() {
        return (List<Customer>) getAllOfObjectTypeFromDB(new Customer());
    }

    public List<Item> getAllItemsFormDB() {
        return (List<Item>) getAllOfObjectTypeFromDB(new Item());
    }

    public List<Order> getAllOrdersFromDB() {
        return (List<Order>) getAllOfObjectTypeFromDB(new Order());
    }

    public List<OrderLine> getAllOrderLinesFromDB() {
        return (List<OrderLine>) getAllOfObjectTypeFromDB(new OrderLine());
    }


}
