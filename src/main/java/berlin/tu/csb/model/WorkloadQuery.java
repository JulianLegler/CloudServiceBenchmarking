package berlin.tu.csb.model;

public class WorkloadQuery {
    public String sqlString;
    public long workloadContextId;
    public long executingOrderId;
    public String timestampBeforeCommit;
    public String timestampAfterCommit;
}
