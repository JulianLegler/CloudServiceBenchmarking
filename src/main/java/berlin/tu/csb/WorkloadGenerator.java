package berlin.tu.csb;

import org.apache.commons.lang3.RandomUtils;

import java.util.List;


/**
 * Main class for the basic JDBC example.
 **/
public class WorkloadGenerator implements Runnable {

    BenchmarkDAO dao;
    long startTime;
    long runTimeInSeconds;
    long endTime;

    public WorkloadGenerator(BenchmarkDAO dao, long startTime, long runTimeInSeconds, long endTime) {
        this.dao = dao;
        this.startTime = startTime;
        this.runTimeInSeconds = runTimeInSeconds;
        this.endTime = endTime;
    }

    @Override
    public void run() {
        while (System.currentTimeMillis() < startTime) {
            try {
                Thread.sleep(10);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("thread started");
        while (System.currentTimeMillis() < endTime) {
            Customer customer = dao.insertRandomCustomer();
            //System.out.println("inserted customer " + customer.c_id);
            Orders order = dao.insertRandomOrder(customer);
            //System.out.println("inserted order " + order.o_id);
            List<Item> itemList = dao.getRandomItems(RandomUtils.nextInt(1, 10));
            List<OrderLine> orderLine = dao.insertOrderLine(order, itemList);
            //System.out.println("added items to order " + orderLine.size());
        }
        System.out.println("thread finished");
    }
}

