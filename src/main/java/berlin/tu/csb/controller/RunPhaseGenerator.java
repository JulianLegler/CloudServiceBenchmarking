package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.ArrayList;


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

        logger.info("Fetching Customers from DB...");
        persistenceController.syncCustomerStateWithDB();
        logger.info("Fetched " + persistenceController.stateController.getCustomerListSize() + " Customer from DB.");

        logger.info("Fetching Items from DB...");
        persistenceController.syncItemStateWithDB();
        logger.info("Fetched " + persistenceController.stateController.getItemListSize() + " Items from DB.");


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
        addToProbabilityList(95, new fetchRandomItem(), probabilityArray);
        addToProbabilityList(5, new fetchRandomCustomer(), probabilityArray);

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

    private class fetchRandomCustomer implements Runnable {
        @Override
        public void run() {
            Customer customer = persistenceController.databaseController.getCustomer(persistenceController.stateController.getRandomCustomer());
            logger.info("Requested random Customer " +  customer.c_id + " from DB.");
        }
    }

    private void addToProbabilityList(int percentage, Runnable function, ArrayList<Runnable> probabilityList) {
        for (int i = 0; i < percentage; i++) {
            probabilityList.add(function);
        }
    }

    /*
    private boolean fetchRandomItems() {
        return persistenceController.databaseController.getItems(persistenceController.stateController.getRandomItems(seededRandomHelper.getIntBetween(1, 20))).size() != 0;
    }

     */

    /*
    private boolean fetchRandomCustomer() {
        return persistenceController.databaseController.getCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

     */

    private boolean fetchOrdersFromRandomCustomer() {
        return persistenceController.databaseController.getOrdersOfCustomer(persistenceController.stateController.getRandomCustomer()) != null;
    }

}

