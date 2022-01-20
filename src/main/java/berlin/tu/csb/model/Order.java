package berlin.tu.csb.model;


import berlin.tu.csb.controller.SeededRandomHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

public class Order implements DatabaseTableModel{
    public String o_id;
    public String c_id;
    public Timestamp o_date;
    public float o_sub_total;
    public float o_tax;
    public float o_total;
    public String o_ship_type;
    public Timestamp o_ship_date;
    public String o_ship_addr;
    public String o_status;

    public Order() {

    }

    public DatabaseTableModel setRandomValues(SeededRandomHelper seededRandomHelper) {
        o_id = UUID.randomUUID().toString();
        c_id = null;
        o_date = new Timestamp(seededRandomHelper.getLongBetween(1577833200000L, 1641141846417L)); //2020 - now
        o_sub_total = seededRandomHelper.getFloatBetween(5f, 12000f);
        o_tax = o_sub_total*0.19f;
        o_total = o_sub_total+o_tax;
        o_ship_type = seededRandomHelper.getStringWithLength(3, 10);
        o_ship_date = new Timestamp(o_date.getTime() + seededRandomHelper.getLongBetween(172800000L, 432000000L)); // 1-5 days after order
        o_ship_addr = seededRandomHelper.getStringWithLength(25, 100);
        o_status = seededRandomHelper.getStringWithLength(2, 16);
        return this;
    }

    public void fillStatement(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, o_id);
        preparedStatement.setString(2, c_id);
        preparedStatement.setTimestamp(3, o_date);
        preparedStatement.setFloat(4, o_sub_total);
        preparedStatement.setFloat(5, o_tax);
        preparedStatement.setFloat(6, o_total);
        preparedStatement.setString(7, o_ship_type);
        preparedStatement.setTimestamp(8, o_ship_date);
        preparedStatement.setString(9, o_ship_addr);
        preparedStatement.setString(10, o_status);
    }

    public void initWithResultSet(ResultSet resultSet) throws SQLException {
        o_id = resultSet.getString("o_id");
        c_id = resultSet.getString("c_id");
        o_date = resultSet.getTimestamp("o_date");
        o_sub_total = resultSet.getFloat("o_sub_total");
        o_tax = resultSet.getFloat("o_tax");
        o_total = resultSet.getFloat("o_total");
        o_ship_type = resultSet.getString("o_ship_type");
        o_ship_date = resultSet.getTimestamp("o_ship_date");
        o_ship_addr = resultSet.getString("o_ship_addr");
        o_status = resultSet.getString("o_status");
    }

    @Override
    public String getSQLInsertString() {
        return "INSERT INTO orders (o_id, c_id, o_date, o_sub_total, o_tax, o_total, o_ship_type, o_ship_date, o_ship_addr, o_status) VALUES (?, ?, ?, ?, ?, ? ,?, ?, ?, ?)";
    }

    @Override
    public String getBasicSQLSelfSelectString() {
        return String.format("SELECT * FROM orders WHERE o_id = '%s'", o_id);
    }

    @Override
    public String getBasicSQLAllSelectString() {
        return String.format("SELECT * FROM orders");
    }

    @Override
    public String toString() {
        return "Orders{" +
                "o_id='" + o_id + '\'' +
                ", c_id='" + c_id + '\'' +
                ", o_date=" + o_date +
                ", o_sub_total=" + o_sub_total +
                ", o_tax=" + o_tax +
                ", o_total=" + o_total +
                ", o_ship_type='" + o_ship_type + '\'' +
                ", o_ship_date=" + o_ship_date +
                ", o_ship_addr='" + o_ship_addr + '\'' +
                ", o_status='" + o_status + '\'' +
                '}';
    }
}
