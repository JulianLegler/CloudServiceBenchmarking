package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.ArrayList;
import java.util.List;

public class WorkerGeneratorController {
    StateController stateController;

    public WorkerGeneratorController(StateController stateController) {
        this.stateController = stateController;
    }

    public Customer getNewCustomerModelWithRandomData() {
        Customer customer = new Customer().setRandomCustomerValues();
        stateController.addCustomer(customer);
        return customer;
    }

    public Order getNewOrderModelWithRandomData(Customer customer) {
        Order order = new Order().setRandomValues(customer.c_id);
        stateController.addOrder(order);
        return order;
    }

    public Item getNewItemModelWithRandomData() {
        Item item = new Item().setRandomValues();
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
            OrderLine orderLine = new OrderLine().setRandomValues(order.o_id, item.i_id);
            stateController.addOrderLine(orderLine);
            orderLineList.add(orderLine);
        }
        return orderLineList;
    }
}
