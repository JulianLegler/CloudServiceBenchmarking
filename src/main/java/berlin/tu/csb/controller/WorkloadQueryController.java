package berlin.tu.csb.controller;

import berlin.tu.csb.model.WorkloadQuery;

import java.util.ArrayList;

public class WorkloadQueryController {

    ArrayList<WorkloadQuery> workloadQueryList = new ArrayList<>();
    long workloadQueryIncrementalId = 0;
    long workloadContextId;

    public void add(String sqlString) {
        workloadContextId = Thread.currentThread().getId();
        WorkloadQuery workloadQuery = new WorkloadQuery();
        workloadQuery.sqlString = sqlString;
        workloadQuery.workloadContextId = workloadContextId;
        workloadQuery.executingOrderId = workloadQueryIncrementalId++;

        workloadQueryList.add(workloadQuery);
    }
}
