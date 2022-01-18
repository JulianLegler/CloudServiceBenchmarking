package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;
import org.apache.commons.lang3.RandomUtils;

import java.util.List;


/**
 * Main class for the basic JDBC example.
 **/
public class WorkloadGenerator implements Runnable {

    long startTime;
    long runTimeInSeconds;
    long endTime;
    WorkerGeneratorController workerGeneratorController;
    SeededRandomHelper seededRandomHelper;
    PersistenceController persistenceController;

    public WorkloadGenerator(PersistenceController persistenceController, SeededRandomHelper seededRandomHelper, long startTime, long runTimeInSeconds, long endTime) {
        this.persistenceController = persistenceController;
        this.workerGeneratorController = new WorkerGeneratorController(seededRandomHelper);
        this.seededRandomHelper = seededRandomHelper;
        this.startTime = startTime;
        this.runTimeInSeconds = runTimeInSeconds;
        this.endTime = endTime;
    }

    @Override
    public void run() {
        // Coordination Phase: All threads wait until the set time has come
        while (System.currentTimeMillis() < startTime) {
            try {
                Thread.sleep(100);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("thread started");
        while (System.currentTimeMillis() < endTime) {

            for(int i = 0; i < 2; i++) {
                insertNewData();
            }

            int differentCases = SeededRandomHelper.getIntBetween(0, 4);
            switch (differentCases) {
                case 0:
                    System.out.println("Running case 0 - create and insert new customer, order with lines and items to db");
                    if(!insertNewData()) {
                        System.out.println("Error while running insertNewData");
                    }
                    break;
                case 1:
                    System.out.println("Running case 1 - create and insert new items to db");
                    if(!insertNewItems()) {
                        System.out.println("Error while running insertNewItems");
                    }
                    break;
                case 2:
                    System.out.println("Running case 2 - create and insert new order with lines for existing user and items to db");
                    if(!placeNewOrderForExistingCustomer()) {
                        System.out.println("Error while running placeNewOrderForExistingCustomer");
                    }
                    break;
                case 3:
                    System.out.println("Running case 3 - create and insert new customer, order with lines for existing items to db");
                    if(!placeNewOrderForNewCustomer()) {
                        System.out.println("Error while running placeNewOrderForNewCustomer");
                    }
                    break;
                default:
                    System.out.println("Default Case reached, something is wrong?");
            }

        }
        System.out.println("thread finished");
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
}

