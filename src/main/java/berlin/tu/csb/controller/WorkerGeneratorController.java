package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.ArrayList;
import java.util.List;

public class WorkerGeneratorController {
    StateController stateController;
    SeededRandomHelper seededRandomHelper;

    public WorkerGeneratorController(StateController stateController, SeededRandomHelper seededRandomHelper) {
        this.stateController = stateController;
        this.seededRandomHelper = seededRandomHelper;
    }

    public Customer getNewCustomerModelWithRandomData() {
        Customer customer = new Customer().setRandomCustomerValues(seededRandomHelper);
        stateController.addCustomer(customer);
        return customer;
    }

    public Order getNewOrderModelWithRandomData(Customer customer) {
        Order order = new Order().setRandomValues(customer.c_id, seededRandomHelper);
        stateController.addOrder(order);
        return order;
    }

    public Item getNewItemModelWithRandomData() {
        Item item = new Item().setRandomValues(seededRandomHelper);
        stateController.addItem(item);
        return item;
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
            stateController.addOrderLine(orderLine);
            orderLineList.add(orderLine);
        }
        return orderLineList;
    }
}
