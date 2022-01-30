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

    private SeededRandomHelper seededRandomHelper;

    public StateController(SeededRandomHelper seededRandomHelper) {
        this.seededRandomHelper = seededRandomHelper;
    }

    public int getCustomerListSize() {
        return customerIds.size();
    }

    public int getItemListSize() {
        return itemIds.size();
    }

    public int getOrderSize() {
        return orderIds.size();
    }

    public int getOrderLineSize() {return orderLineIds.size();}



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
        String randomCustomerId = customerIds.get(seededRandomHelper.getIntBetween(0, customerIds.size()-1));
        return customerMap.get(randomCustomerId);
    }

    public Item getRandomItem() {
        String randomItemId = itemIds.get(seededRandomHelper.getIntBetween(0, itemIds.size()-1));
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
            itemList = new ArrayList<>(itemMap.values());
        }
        return itemList;
    }

    public Order getRandomOrder() {
        String randomOrderId = orderIds.get(seededRandomHelper.getIntBetween(0, orderIds.size()-1));
        return orderMap.get(randomOrderId);
    }

    public boolean hasCustomer() {
        return !customerIds.isEmpty();
    }

    public boolean hasItems() {
        return !itemIds.isEmpty();
    }

    public void setState(StateController s) {
        this.customerMap = s.customerMap;
        this.customerIds = s.customerIds;
        this.itemMap = s.itemMap;
        this.itemIds = s.itemIds;
        this.orderMap = s.orderMap;
        this.orderIds = s.orderIds;
        this.orderLineMap = s.orderLineMap;
    }
}
