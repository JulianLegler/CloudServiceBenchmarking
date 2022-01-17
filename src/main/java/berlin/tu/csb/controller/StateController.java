package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.*;

public class StateController {
    private Map<String, Customer> customerMap = new HashMap<>();
    private Map<String, Item> itemMap = new HashMap<>();
    private Map<String, Order> orderMap = new HashMap<>();
    private Map<String, OrderLine> orderLineMap = new HashMap<>();

    private ArrayList<String> customerIds = new ArrayList<>();
    private ArrayList<String> itemIds = new ArrayList<>();
    private ArrayList<String> orderIds = new ArrayList<>();
    private ArrayList<String> orderLineIds = new ArrayList<>();


    public void addCustomer(Customer customer) {
        customerMap.put(customer.c_id, customer);
        customerIds.add(customer.c_id);
    }

    public void addItem(Item item) {
        itemMap.put(item.i_id, item);
        itemIds.add(item.i_id);
    }

    public void addOrder(Order order) {
        orderMap.put(order.o_id, order);
        orderIds.add(order.o_id);
    }

    public void addOrderLine(OrderLine orderLine) {
        orderLineMap.put(orderLine.ol_id, orderLine);
        orderLineIds.add(orderLine.ol_id);
    }

    public Customer getRandomCustomer() {
        String randomCustomerId = customerIds.get(SeededRandomHelper.getIntBetween(0, customerIds.size()-1));
        return customerMap.get(randomCustomerId);
    }

    public Item getRandomItem() {
        String randomItemId = itemIds.get(SeededRandomHelper.getIntBetween(0, itemIds.size()-1));
        return itemMap.get(randomItemId);
    }

    public List<Item> getRandomItems(int amount) {
        List<Item> itemList = new ArrayList<>();

        if(itemIds.size() > amount) {
            while (itemList.size() < amount) {
                Item randomItem = getRandomItem();
                if(!itemList.contains(randomItem)) {
                    itemList.add(randomItem);
                }
            }
        }
        else {
            itemList = (List<Item>) itemMap.values();
        }
        return itemList;
    }

    public Order getRandomOrder() {
        String randomOrderId = orderIds.get(SeededRandomHelper.getIntBetween(0, orderIds.size()-1));
        return orderMap.get(randomOrderId);
    }

    public boolean hasCustomer() {
        return !customerIds.isEmpty();
    }

    public boolean hasItems() {
        return !itemIds.isEmpty();
    }
}
