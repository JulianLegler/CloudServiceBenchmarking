package berlin.tu.csb.controller;

import berlin.tu.csb.model.Customer;
import berlin.tu.csb.model.Item;
import berlin.tu.csb.model.Order;
import berlin.tu.csb.model.OrderLine;

import java.util.List;

public class PersistenceController {
    DatabaseController databaseController;
    StateController stateController;

    public PersistenceController(DatabaseController databaseController, StateController stateController) {
        this.databaseController = databaseController;
        this.stateController = stateController;
    }

    public boolean insertCustomer(Customer customer) {
        if(databaseController.insertCustomer(customer)) {
            stateController.addCustomer(customer);
            return true;
        }
        else
            return false;
    }

    public boolean insertItem(Item item) {
        if(databaseController.insertItem(item)) {
            stateController.addItem(item);
            return true;
        }
        else
            return false;
    }

    private boolean insertOrder(Order order) {
        if(databaseController.insertOrder(order)) {
            stateController.addOrder(order);
            return true;
        }
        else
            return false;
    }

    private boolean insertOrderLines(List<OrderLine> orderLineList) {
        if(databaseController.insertOrderLines(orderLineList)) {
            for (OrderLine orderLine: orderLineList) {
                stateController.addOrderLine(orderLine);
            }
            return true;
        }
        else
            return false;
    }

    public boolean insertOrderWithOrderLines(Order order, List<OrderLine> orderLineList) {
        if(insertOrder(order) && insertOrderLines(orderLineList)) {
            stateController.addOrder(order);
            for (OrderLine orderLine: orderLineList) {
                stateController.addOrderLine(orderLine);
            }
            return true;
        }
        else
            return false;
    }



}
