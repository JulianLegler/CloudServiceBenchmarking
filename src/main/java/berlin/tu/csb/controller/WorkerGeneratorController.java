package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.ArrayList;
import java.util.List;

public class WorkerGeneratorController {
    SeededRandomHelper seededRandomHelper;

    public WorkerGeneratorController(SeededRandomHelper seededRandomHelper) {
        this.seededRandomHelper = seededRandomHelper;
    }

    public Customer getNewCustomerModelWithRandomData() {
        return new Customer().setRandomCustomerValues(seededRandomHelper);
    }

    public Order getNewOrderModelWithRandomData(Customer customer) {
        return new Order().setRandomValues(customer.c_id, seededRandomHelper);
    }

    public Item getNewItemModelWithRandomData() {
        return new Item().setRandomValues(seededRandomHelper);
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
            OrderLine orderLine = new OrderLine().setRandomValues(order.o_id, item.i_id, seededRandomHelper);
            orderLineList.add(orderLine);
        }
        return orderLineList;
    }
}
