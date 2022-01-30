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
        while (persistenceController.stateController.getCustomerListSize() < benchmarkConfig.dbCustomerInsertsLoadPhase / benchmarkConfig.threadCountLoad
                || persistenceController.stateController.getItemListSize() < benchmarkConfig.dbItemInsertsLoadPhase / benchmarkConfig.threadCountLoad
                || persistenceController.stateController.getOrderSize() < benchmarkConfig.dbOrderInsertsLoadPhase / benchmarkConfig.threadCountLoad) {

            if(persistenceController.stateController.getCustomerListSize() < benchmarkConfig.dbCustomerInsertsLoadPhase / benchmarkConfig.threadCountLoad) {
                logger.info("Inserting " + benchmarkConfig.dbCustomerInsertsLoadPhase / benchmarkConfig.threadCountLoad + " Customer to the DB");
                if(!workerGeneratorController.bulkInsertNewCustomers(benchmarkConfig.dbCustomerInsertsLoadPhase / benchmarkConfig.threadCountLoad)) {
                    logger.error("Error while running bulkInsertNewCustomers");
                }
                continue;
            }

            if(persistenceController.stateController.getItemListSize() < benchmarkConfig.dbItemInsertsLoadPhase / benchmarkConfig.threadCountLoad) {
                logger.info("Inserting " + benchmarkConfig.dbItemInsertsLoadPhase / benchmarkConfig.threadCountLoad + " Items to the DB");
                if(!workerGeneratorController.bulkInsertNewItems(benchmarkConfig.dbItemInsertsLoadPhase / benchmarkConfig.threadCountLoad)) {
                    logger.error("Error while running bulkInsertNewItems");
                }
                continue;
            }

            if(persistenceController.stateController.getOrderSize() < benchmarkConfig.dbOrderInsertsLoadPhase / benchmarkConfig.threadCountLoad) {
                logger.info("Inserting " + benchmarkConfig.dbOrderInsertsLoadPhase / benchmarkConfig.threadCountLoad + " Orders to the DB");
                if(!workerGeneratorController.bulkInsertNewOrdersWithOrderLines(benchmarkConfig.dbOrderInsertsLoadPhase / benchmarkConfig.threadCountLoad)) {
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

