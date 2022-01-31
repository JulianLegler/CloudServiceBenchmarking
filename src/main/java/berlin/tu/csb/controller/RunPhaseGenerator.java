package berlin.tu.csb.controller;

import berlin.tu.csb.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Main class for the basic JDBC example.
 **/
public class RunPhaseGenerator implements Runnable {

    long startTime;
    long runTimeInSeconds;
    long endTime;
    WorkerGeneratorController workerGeneratorController;
    SeededRandomHelper seededRandomHelper;
    PersistenceController persistenceController;
    Logger logger = LogManager.getLogger(RunPhaseGenerator.class);
    BenchmarkConfig benchmarkConfig;

    ArrayList<Item> topSellerItems;

    public RunPhaseGenerator(PersistenceController persistenceController, SeededRandomHelper seededRandomHelper, long startTime, long runTimeInSeconds, long endTime, BenchmarkConfig benchmarkConfig) {
        this.persistenceController = persistenceController;
        this.workerGeneratorController = new WorkerGeneratorController(seededRandomHelper, persistenceController);
        this.seededRandomHelper = seededRandomHelper;
        this.startTime = startTime;
        this.runTimeInSeconds = runTimeInSeconds;
        this.endTime = endTime;
        this.benchmarkConfig = benchmarkConfig;
    }


    @Override
    public void run() {
        // Add some thread specific informations for logging and collection of the SQL statements
        ThreadContext.put("threadName", Thread.currentThread().getName());


        // Creates a List of "Top Seller Items" that will be more frequently searched than others
        topSellerItems = new ArrayList<>(persistenceController.stateController.getRandomItems((int) (persistenceController.stateController.getItemListSize() * 0.1)));





        // Coordination Phase: All threads wait until the set time has come
        while (System.currentTimeMillis() < startTime) {
            try {
                Thread.sleep(100);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.trace("thread started");
        //logger.error("Test error");


        // Create a Array filled with the methods that should run. Place them in there multiple times depending on the probability they should get picked afterwards
        ArrayList<Runnable> probabilityArray = new ArrayList<>();

        // Go through the map and create objects for the configured methods that are in inner classes, so it is more complicated to instantiate
        benchmarkConfig.useCasesProbabilityDistribution.forEach((key, value) -> {
            try {
                Class classNameOfInnerClass = Class.forName(key);
                Constructor constructorForNewInnerClass = classNameOfInnerClass.getDeclaredConstructor(RunPhaseGenerator.class);
                Object newObjectOfInnerClass = constructorForNewInnerClass.newInstance(this);
                addToProbabilityList(value, (Runnable) newObjectOfInnerClass, probabilityArray);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error while creating object from class " + key + " for probability list from config.");
            }
        });

        if(probabilityArray.size() != 100) {
            logger.warn(String.format("Summed probability is %d and not 100!", probabilityArray.size()));
        }


        while (System.currentTimeMillis() < endTime) {

            int dice = seededRandomHelper.getIntBetween(0, 99);

            probabilityArray.get(dice).run();



        }
        logger.trace("thread finished");
        ThreadContext.clearMap();
    }

    private class fetchRandomItem implements Runnable {
        @Override
        public void run() {
            Item item = persistenceController.databaseController.getItem(persistenceController.stateController.getRandomItem());
            logger.info("Requested random Item " + item.i_id + " from DB.");
        }
    }

    private class fetchRandomTopSellerItem implements Runnable {
        @Override
        public void run() {
            Item item = persistenceController.databaseController.getItem(topSellerItems.get(seededRandomHelper.getIntBetween(0, topSellerItems.size()-1)));
            logger.info("Requested top seller Item " + item.i_id + " from DB.");
        }
    }

    private class fetchRandomCustomer implements Runnable {
        @Override
        public void run() {
            Customer customer = persistenceController.databaseController.getCustomer(persistenceController.stateController.getRandomCustomer());
            logger.info("Requested random Customer " +  customer.c_id + " from DB.");
        }
    }

    private class fetchOrdersFromRandomCustomer implements Runnable {
        @Override
        public void run() {
            List<Order> orderList = persistenceController.databaseController.getOrdersOfCustomer(persistenceController.stateController.getRandomCustomer());
            logger.info("Requested list of all orders for a random customer and retrieved " +  orderList.size() + " orders from the DB.");
        }
    }

    private class fetchOrderLinesFromRandomOrder implements Runnable {
        @Override
        public void run() {
            List<OrderLine> orderLineList = persistenceController.databaseController.getOrderLinesOfOrder(persistenceController.stateController.getRandomOrder());
            logger.info("Requested list of all orderslines for the a random order " + orderLineList.get(0).o_id + " and retrieved " +  orderLineList.size() + " orders from the DB.");
        }
    }

    private class fetchItemsSortedByPrice implements Runnable {
        @Override
        public void run() {
            int limit = 50;
            List<Item> itemList = persistenceController.databaseController.getItemsSortedByPrice(limit);
            logger.info("Requested list of " + limit + " Items ordered by their price and received " +  itemList.size() + " items from the DB.");
        }
    }

    private class fetchItemsSortedByName implements Runnable {
        @Override
        public void run() {
            int limit = 50;
            List<Item> itemList = persistenceController.databaseController.getItemsSortedByName(limit);
            logger.info("Requested list of " + limit + " Items ordered by their name and received " +  itemList.size() + " items from the DB.");
        }
    }

    private class fetchItemsWithStringInName implements Runnable {
        @Override
        public void run() {
            int limit = 50;
            String randomTitle = persistenceController.stateController.getRandomItem().i_title;
            int lowerIndex = seededRandomHelper.getIntBetween(0, randomTitle.length());
            int upperIndex = seededRandomHelper.getIntBetween(lowerIndex, randomTitle.length());
            String searchName = randomTitle.substring(lowerIndex, upperIndex);
            List<Item> itemList = persistenceController.databaseController.getItemsWithName(limit, searchName);
            logger.info("Requested list of " + limit + " Items that have " + searchName + " in their name and received " +  itemList.size() + " items from the DB.");
        }
    }

    private class insertNewCustomer implements Runnable {
        @Override
        public void run() {
            Customer customer = workerGeneratorController.getNewCustomerModelWithRandomData();
            if(persistenceController.insertCustomer(customer)) {
                logger.info("Inserted a new Customer into the DB");
            } else {
                logger.error("Error while inserting new Customer");
            }
        }
    }

    private class insertNewOrder implements Runnable {
        @Override
        public void run() {
            Order order = workerGeneratorController.getNewOrderModelWithRandomData(persistenceController.stateController.getRandomCustomer());
            List<Item> itemList = persistenceController.stateController.getRandomItems(seededRandomHelper.getIntBetween(1, 10));
            List<OrderLine> orderLineList = workerGeneratorController.getNewOrderLineModelList(order, itemList);
            if(persistenceController.insertOrderWithOrderLines(order, orderLineList)) {
                logger.info("Inserted a new Order with OrderLines into the DB");
            }
            else {
                logger.error("Errow hile inserting new Order with Orderlines");
            }
        }
    }

    private class fetchAllCustomersWithOpenOrders implements Runnable {
        @Override
        public void run() {
            List<Customer> customerList = persistenceController.databaseController.getAllCustomersWithOpenOrders();
            logger.info("Requested list of all customers with open orders and retrieved " +  customerList.size() + " customers from the DB.");
        }
    }

    private class updateItemPrice implements Runnable {
        @Override
        public void run() {
            Item item = persistenceController.stateController.getRandomItem();
            item.i_srp += 10;
            item.i_cost += 10;
            if(persistenceController.updateItemPrice(item)) {
                logger.info("Updated a Items price into the DB");
            }
            else {
                logger.error("Errow hile updating an Items price");
            }
        }
    }



    private void addToProbabilityList(int percentage, Runnable function, ArrayList<Runnable> probabilityList) {
        for (int i = 0; i < percentage; i++) {
            probabilityList.add(function);
        }
    }



}

