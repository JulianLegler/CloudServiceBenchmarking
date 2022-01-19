package berlin.tu.csb.controller;

import berlin.tu.csb.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.List;


/**
 * Main class for the basic JDBC example.
 **/
public class LoadPhaseGenerator implements Runnable {

    WorkerGeneratorController workerGeneratorController;
    SeededRandomHelper seededRandomHelper;
    PersistenceController persistenceController;
    Logger logger = LogManager.getLogger(LoadPhaseGenerator.class);

    BenchmarkConfig benchmarkConfig;

    public LoadPhaseGenerator(PersistenceController persistenceController, SeededRandomHelper seededRandomHelper, BenchmarkConfig benchmarkConfig) {
        this.persistenceController = persistenceController;
        this.workerGeneratorController = new WorkerGeneratorController(seededRandomHelper, persistenceController);
        this.seededRandomHelper = seededRandomHelper;
        this.benchmarkConfig = benchmarkConfig;
    }

    @Override
    public void run() {
        // Add some thread specific informations for logging and collection of the SQL statements
        ThreadContext.put("threadName", Thread.currentThread().getName());

        // Coordination Phase: All threads wait until the set time has come
        logger.info("thread started");
        //logger.error("Test error");
        while (persistenceController.stateController.getCustomerListSize() < benchmarkConfig.dbCustomerInsertsLoadPhase
                || persistenceController.stateController.getItemListSize() < benchmarkConfig.dbItemInsertsLoadPhase
                || persistenceController.stateController.getOrderSize() < benchmarkConfig.dbOrderInsertsLoadPhase) {

            if(persistenceController.stateController.getCustomerListSize() < benchmarkConfig.dbCustomerInsertsLoadPhase) {
                logger.info("Inserting " + benchmarkConfig.dbCustomerInsertsLoadPhase + " Customer to the DB");
                if(!workerGeneratorController.bulkInsertNewCustomers(benchmarkConfig.dbCustomerInsertsLoadPhase)) {
                    logger.error("Error while running bulkInsertNewCustomers");
                }
                continue;
            }

            if(persistenceController.stateController.getItemListSize() < benchmarkConfig.dbItemInsertsLoadPhase) {
                logger.info("Inserting " + benchmarkConfig.dbItemInsertsLoadPhase + " Items to the DB");
                if(!workerGeneratorController.bulkInsertNewItems(benchmarkConfig.dbItemInsertsLoadPhase)) {
                    logger.error("Error while running bulkInsertNewItems");
                }
                continue;
            }

            if(persistenceController.stateController.getOrderSize() < benchmarkConfig.dbOrderInsertsLoadPhase) {
                logger.info("Inserting " + benchmarkConfig.dbOrderInsertsLoadPhase + " Orders to the DB");
                if(!workerGeneratorController.bulkInsertNewOrdersWithOrderLines(benchmarkConfig.dbOrderInsertsLoadPhase)) {
                    logger.error("Error while running bulkInsertNewOrdersWithOrderLines");
                }
                continue;
            }
            break;
        }
        logger.info("thread finished");
        ThreadContext.clearMap();
    }

}

