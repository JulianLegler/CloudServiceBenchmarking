package berlin.tu.csb.model;

import java.util.Map;

public class BenchmarkConfig {
    public long dbCustomerInsertsLoadPhase;
    public long dbItemInsertsLoadPhase;
    public long dbOrderInsertsLoadPhase;
    public int threadCountLoad;
    public int threadCountRun;
    public long seed;
    public Map<String, Integer> useCasesProbabilityDistribution;
    public int minRunTimeOfRunPhaseInMinutes;
    public int initialWaitTimeForCoordinationInSeconds;
}
