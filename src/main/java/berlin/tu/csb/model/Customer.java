package berlin.tu.csb.model;

import berlin.tu.csb.controller.SeededRandomHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class Customer {
    public String c_id;
    public String c_business_name;
    public String c_business_info;
    public String c_passwd;
    public String c_contact_fname;
    public String c_contact_lname;
    public String c_addr;
    public String c_contact_phone;
    public String c_contact_email;
    public String c_payment_method;
    public String c_credit_info;
    public float c_discount;

    public Customer() {
    }

    public Customer setRandomCustomerValues() {
        c_id = UUID.randomUUID().toString();
        c_business_name = SeededRandomHelper.getStringWithLength(5, 20);
        c_business_info = SeededRandomHelper.getStringWithLength(20, 100);
        c_passwd = SeededRandomHelper.getStringWithLength(5, 20);
        c_contact_fname = SeededRandomHelper.getStringWithLength(5, 15);
        c_contact_lname = SeededRandomHelper.getStringWithLength(5, 15);
        c_addr = SeededRandomHelper.getStringWithLength(30, 100);
        c_contact_phone = String.valueOf(SeededRandomHelper.getLongBetween(8, 16));
        c_contact_email = SeededRandomHelper.getStringWithLength(7, 35);
        c_payment_method = SeededRandomHelper.getStringWithLength(1, 2);
        c_credit_info = SeededRandomHelper.getStringWithLength(20,300);
        c_discount = SeededRandomHelper.getFloatBetween(0, 35);
        return this;
    }

    public void fillStatement(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, c_id);
        preparedStatement.setString(2, c_business_name);
        preparedStatement.setString(3, c_business_info);
        preparedStatement.setString(4, c_passwd);
        preparedStatement.setString(5, c_contact_fname);
        preparedStatement.setString(6, c_contact_lname);
        preparedStatement.setString(7, c_addr);
        preparedStatement.setString(8, c_contact_phone);
        preparedStatement.setString(9, c_contact_email);
        preparedStatement.setString(10, c_payment_method);
        preparedStatement.setString(11, c_credit_info);
        preparedStatement.setFloat(12, c_discount);
    }

    public void initWithResultSet(ResultSet resultSet) throws SQLException {
        c_id = resultSet.getString("c_id");
        c_business_name = resultSet.getString("c_business_name");
        c_business_info = resultSet.getString("c_business_info");
        c_passwd = resultSet.getString("c_passwd");
        c_contact_fname = resultSet.getString("c_contact_fname");
        c_contact_lname = resultSet.getString("c_contact_lname");
        c_addr = resultSet.getString("c_addr");
        c_contact_phone = resultSet.getString("c_contact_phone");
        c_contact_email = resultSet.getString("c_contact_email");
        c_payment_method = resultSet.getString("c_payment_method");
        c_credit_info = resultSet.getString("c_credit_info");
        c_discount = resultSet.getFloat("c_discount");
    }

    @Override
    public String toString() {
        return "Customer{" +
                "c_id='" + c_id + '\'' +
                ", c_business_name='" + c_business_name + '\'' +
                ", c_business_info='" + c_business_info + '\'' +
                ", c_passwd='" + c_passwd + '\'' +
                ", c_contact_fname='" + c_contact_fname + '\'' +
                ", c_contact_lname='" + c_contact_lname + '\'' +
                ", c_addr='" + c_addr + '\'' +
                ", c_contact_phone='" + c_contact_phone + '\'' +
                ", c_contact_email='" + c_contact_email + '\'' +
                ", c_payment_method='" + c_payment_method + '\'' +
                ", c_credit_info='" + c_credit_info + '\'' +
                ", c_discount=" + c_discount +
                '}';
    }
}
