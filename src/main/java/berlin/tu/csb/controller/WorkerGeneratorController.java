package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.ArrayList;
import java.util.List;

public class WorkerGeneratorController {
    SeededRandomHelper seededRandomHelper;
    PersistenceController persistenceController;

    public WorkerGeneratorController(SeededRandomHelper seededRandomHelper, PersistenceController persistenceController) {
        this.seededRandomHelper = seededRandomHelper;
        this.persistenceController = persistenceController;
    }

    public Customer getNewCustomerModelWithRandomData() {
        return (Customer) new Customer().setRandomValues(seededRandomHelper);
    }

    public Order getNewOrderModelWithRandomData(Customer customer) {
        Order order = (Order) new Order().setRandomValues(seededRandomHelper);
        order.c_id = customer.c_id;
        return order;
    }

    public Item getNewItemModelWithRandomData() {
        return (Item) new Item().setRandomValues(seededRandomHelper);
    }

    public List<Item> getNewItemModelListWithRandomData(int itemListSize) {
        List<Item> itemList = new ArrayList<>();
        for (int i = 0; i < itemListSize; i++) {
            itemList.add(getNewItemModelWithRandomData());
        }
        return itemList;
    }

    public List<OrderLine> getNewOrderLineModelList(Order order, List<Item> itemList) {
        List<OrderLine> orderLineList = new ArrayList<>();
        for (Item item:itemList) {
            OrderLine orderLine = new OrderLine().setRandomValues(seededRandomHelper);
            orderLine.i_id = item.i_id;
            orderLine.o_id = order.o_id;
            orderLineList.add(orderLine);
        }
        return orderLineList;
    }


    public boolean placeNewOrderForNewCustomer() {

        /*
         *
         * Phase 1: generate and gather data
         *
         */

        // Create a new customer
        Customer customer = getNewCustomerModelWithRandomData();

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
        Order order = getNewOrderModelWithRandomData(customer);
        // create new order lines for each the newly created order with the fetched items
        List<OrderLine> orderLineList = getNewOrderLineModelList(order, itemList);

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

    public boolean placeNewOrderForExistingCustomer() {
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
        Order order = getNewOrderModelWithRandomData(customer);
        // create new order lines for each the newly created order with the fetched items
        List<OrderLine> orderLineList = getNewOrderLineModelList(order, itemList);

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

    public boolean insertNewData() {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        // create a new customer, a new order, new items and then new orderlines with the created data
        Customer customer = getNewCustomerModelWithRandomData();
        Order order = getNewOrderModelWithRandomData(customer);
        List<Item> itemList = getNewItemModelListWithRandomData(SeededRandomHelper.getIntBetween(1, 10));
        List<OrderLine> orderLineList = getNewOrderLineModelList(order, itemList);

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

    public boolean bulkInsertNewCustomers(long amount) {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        // create a new customer, a new order, new items and then new orderlines with the created data
        List<Customer> customerList = new ArrayList<>();
        for (long i = 0; i < amount; i++) {
            customerList.add(getNewCustomerModelWithRandomData());
        }

        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (!persistenceController.bulkInsertCustomers(customerList)) {
            System.out.printf("Error while inserting Customer list with %d entries", amount);
            isSuccessful = false;

        }

        return isSuccessful;
    }

    public boolean insertNewItems() {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        List<Item> itemList = getNewItemModelListWithRandomData(SeededRandomHelper.getIntBetween(1, 10));


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

    public boolean fetchRandomItem() {
        return persistenceController.databaseController.getItem(persistenceController.stateController.getRandomItem()) != null;
    }

    public boolean fetchRandomItems() {
        return persistenceController.databaseController.getItems(persistenceController.stateController.getRandomItems(seededRandomHelper.getIntBetween(1, 20))).size() != 0;
    }

    public boolean fetchRandomCustomer() {
        return persistenceController.databaseController.getCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

    public boolean fetchOrdersFromRandomCustomer() {
        return persistenceController.databaseController.getOrdersOfCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

    public boolean bulkInsertNewItems(long amount) {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        // create a new customer, a new order, new items and then new orderlines with the created data
        List<Item> itemList = getNewItemModelListWithRandomData((int) amount);

        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (!persistenceController.bulkInsertItems(itemList)) {
            System.out.printf("Error while inserting Item list with %d entries", amount);
            isSuccessful = false;

        }

        return isSuccessful;
    }

    public boolean bulkInsertNewOrdersWithOrderLines(long amount) {
        /*
         *
         * Phase 1: generate or gather data
         *
         */
        // create a new customer, a new order, new items and then new orderlines with the created data


        List<Order> orderList = new ArrayList<>();
        List<OrderLine> orderLineList = new ArrayList<>();



        for (long i = 0; i < amount; i++) {
            Customer customer;
            if(persistenceController.stateController.hasCustomer()) {
                customer = persistenceController.stateController.getRandomCustomer();
            }
            else {
                System.out.println("Error: no customers in local state");
                return false;
            }


            Order order = getNewOrderModelWithRandomData(customer);
            orderList.add(order);


            // Get some existing Items
            List<Item> itemList;
            if(persistenceController.stateController.hasItems()) {
                itemList = persistenceController.stateController.getRandomItems(SeededRandomHelper.getIntBetween(1, 8));
            }
            else {
                System.out.println("Error: no items in local state");
                return false;
            }
            // Add a List of new OrderLine Objects to the existing list
            orderLineList.addAll(getNewOrderLineModelList(order, itemList));
        }


        /*
         *
         * Phase 2: Insert Data to Database
         *
         */
        boolean isSuccessful = true;
        if (!persistenceController.bulkInsterOrdersAndBulkInsertOrderLines(orderList, orderLineList)) {
            System.out.printf("Error while inserting Order and OrderLine list with %d entries", amount);
            isSuccessful = false;
        }

        return isSuccessful;
    }
}
