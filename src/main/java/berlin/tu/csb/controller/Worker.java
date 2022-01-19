package berlin.tu.csb.controller;

import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Worker {
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Incorrect number of arguments: " + args.length + "/2");
            System.out.println("1 - ip address of SUT");
            System.out.println("2 - start time");
        }
        if (args.length == 0) {
            System.out.println("No Argument found. Try to get ip from local file.");
            args = new String[1];
            System.out.println("Present Project Directory : " + System.getProperty("user.dir"));


            String content = "";
            try {
                content = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "\\terraform\\public_ip.csv")));
            } catch (IOException e) {
                System.out.println("public_ip.csv not present. Are the servers set up correctly?");
                e.printStackTrace();
                System.exit(1);
            }

            String[] serverAddresses = content.split(",");
            args[0] = serverAddresses[0];
        }

        System.out.println("Try connecting to database " + args[0]);

        // Configure the database connection.
        //PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{args[0]});
        ds.setPortNumbers(new int[]{26257});
        ds.setDatabaseName("tpc_w_light");
        ds.setUser("root");
        ds.setSsl(false);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("woker-" + args[0]);
        //ds.setConnectTimeout();

        // Create DAO.
        WorkloadQueryController workloadQueryController = new WorkloadQueryController();
        BenchmarkDAO dao = new BenchmarkDAO(ds, workloadQueryController);






    }
}
