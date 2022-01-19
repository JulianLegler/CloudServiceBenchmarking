package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.List;


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

    public RunPhaseGenerator(PersistenceController persistenceController, SeededRandomHelper seededRandomHelper, long startTime, long runTimeInSeconds, long endTime) {
        this.persistenceController = persistenceController;
        this.workerGeneratorController = new WorkerGeneratorController(seededRandomHelper, persistenceController);
        this.seededRandomHelper = seededRandomHelper;
        this.startTime = startTime;
        this.runTimeInSeconds = runTimeInSeconds;
        this.endTime = endTime;
    }

    @Override
    public void run() {
        // Add some thread specific informations for logging and collection of the SQL statements
        ThreadContext.put("threadName", Thread.currentThread().getName());


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
        while (System.currentTimeMillis() < endTime) {

            for(int i = 0; i < 2; i++) {
                insertNewData();
            }

            int differentCases = SeededRandomHelper.getIntBetween(0, 6);
            switch (differentCases) {
                case 0:
                    logger.info("Running case 0 - create and insert new customer, order with lines and items to db");
                    if(!insertNewData()) {
                        logger.error("Error while running insertNewData");
                    }
                    break;
                case 1:
                    logger.info("Running case 1 - create and insert new items to db");
                    if(!insertNewItems()) {
                        logger.error("Error while running insertNewItems");
                    }
                    break;
                case 2:
                    logger.info("Running case 2 - create and insert new order with lines for existing user and items to db");
                    if(!placeNewOrderForExistingCustomer()) {
                        logger.error("Error while running placeNewOrderForExistingCustomer");
                    }
                    break;
                case 3:
                    logger.info("Running case 3 - create and insert new customer, order with lines for existing items to db");
                    if(!placeNewOrderForNewCustomer()) {
                        logger.error("Error while running placeNewOrderForNewCustomer");
                    }
                    break;
                case 4:
                    logger.info("Running case 4 - get a random item from the db");
                    if(!fetchRandomItem()) {
                        logger.error("Error while running fetchRandomItem");
                    }
                    break;
                case 5:
                    logger.info("Running case 5 - get a random customer from the db");
                    if(!fetchRandomCustomer()) {
                        logger.error("Error while running fetchRandomCustomer");
                    }
                    break;
                case 6:
                    logger.info("Running case 6 - get all orders from a random customer from the db");
                    if(!fetchOrdersFromRandomCustomer()) {
                        logger.error("Error while running fetchOrdersFromRandomCustomer");
                    }
                    break;
                case 7:
                    logger.info("Running case 6 - get random items from the db");
                    if(!fetchRandomItems()) {
                        logger.error("Error while running fetchRandomItems");
                    }
                    break;
                default:
                    // TODO: come up with more use cases
                    logger.error("Default Case reached, something is wrong?");
            }

        }
        logger.trace("thread finished");
        ThreadContext.clearMap();
    }

    private boolean placeNewOrderForNewCustomer() {

        /*
         *
         * Phase 1: generate and gather data
         *
         */

        // Create a new customer
        Customer customer = workerGeneratorController.getNewCustomerModelWithRandomData();

        // Get some existing Items
        List<Item> itemList;
        if(persistenceController.stateController.hasItems()) {
            itemList = persistenceController.stateController.getRandomItems(SeededRandomHelper.getIntBetween(1, 8));
        }
        else {
            System.out.println("Error: no items in local state");
            return false;
        }

        // create a new Order object
        Order order = workerGeneratorController.getNewOrderModelWithRandomData(customer);
        // create new order lines for each the newly created order with the fetched items
        List<OrderLine> orderLineList = workerGeneratorController.getNewOrderLineModelList(order, itemList);

        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (!persistenceController.insertCustomer(customer)) {
            System.out.printf("Error while inserting Customer %s", customer);
            isSuccessful = false;

        }
        if (isSuccessful) {
            if(!persistenceController.insertOrderWithOrderLines(order, orderLineList)) {
                System.out.printf("Error while inserting order (with orderlines) %s", order);
                isSuccessful = false;
            }
        }

        return isSuccessful;

    }

    private boolean placeNewOrderForExistingCustomer() {
        /*
         *
         * Phase 1: generate and gather data
         *
         */
        // get an existing customer
        Customer customer;
        if(persistenceController.stateController.hasCustomer()) {
            customer = persistenceController.stateController.getRandomCustomer();
        }
        else {
            System.out.println("Error: no customers in local state");
            return false;
        }

        // Get some existing Items
        List<Item> itemList;
        if(persistenceController.stateController.hasItems()) {
            itemList = persistenceController.stateController.getRandomItems(SeededRandomHelper.getIntBetween(1, 8));
        }
        else {
            System.out.println("Error: no items in local state");
            return false;
        }


        // create a new Order object
        Order order = workerGeneratorController.getNewOrderModelWithRandomData(customer);
        // create new order lines for each the newly created order with the fetched items
        List<OrderLine> orderLineList = workerGeneratorController.getNewOrderLineModelList(order, itemList);

        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (isSuccessful) {
            if(!persistenceController.insertOrderWithOrderLines(order, orderLineList)) {
                System.out.printf("Error while inserting order (with orderlines) %s", order);
                isSuccessful = false;
            }
        }

        return isSuccessful;
    }

    private boolean insertNewData() {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        // create a new customer, a new order, new items and then new orderlines with the created data
        Customer customer = workerGeneratorController.getNewCustomerModelWithRandomData();
        Order order = workerGeneratorController.getNewOrderModelWithRandomData(customer);
        List<Item> itemList = workerGeneratorController.getNewItemModelListWithRandomData(SeededRandomHelper.getIntBetween(1, 10));
        List<OrderLine> orderLineList = workerGeneratorController.getNewOrderLineModelList(order, itemList);

        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (!persistenceController.insertCustomer(customer)) {
            System.out.printf("Error while inserting Customer %s", customer);
            isSuccessful = false;

        }
        if (isSuccessful) {
            for (Item item : itemList) {
                if (!persistenceController.insertItem(item)) {
                    System.out.printf("Error while inserting Item %s", item);
                    isSuccessful = false;
                }
            }
        }
        if (isSuccessful) {
            if(!persistenceController.insertOrderWithOrderLines(order, orderLineList)) {
                System.out.printf("Error while inserting order (with orderlines) %s", order);
                isSuccessful = false;
            }
        }

        return isSuccessful;
    }

    private boolean insertNewItems() {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        List<Item> itemList = workerGeneratorController.getNewItemModelListWithRandomData(SeededRandomHelper.getIntBetween(1, 10));


        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;

        for (Item item : itemList) {
            if (!persistenceController.insertItem(item)) {
                System.out.printf("Error while inserting Item %s", item);
                isSuccessful = false;
            }
        }


        return isSuccessful;
    }

    private boolean fetchRandomItem() {
        return persistenceController.databaseController.getItem(persistenceController.stateController.getRandomItem()) != null;
    }

    private boolean fetchRandomItems() {
        return persistenceController.databaseController.getItems(persistenceController.stateController.getRandomItems(seededRandomHelper.getIntBetween(1, 20))).size() != 0;
    }

    private boolean fetchRandomCustomer() {
        return persistenceController.databaseController.getCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

    private boolean fetchOrdersFromRandomCustomer() {
        return persistenceController.databaseController.getOrdersOfCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

}

