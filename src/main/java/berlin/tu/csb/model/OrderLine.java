package berlin.tu.csb.model;

import berlin.tu.csb.controller.SeededRandomHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.UUID;

public class OrderLine implements DatabaseTableModel{
    public String ol_id;
    public String o_id;
    public String i_id;
    public int ol_qty;
    public float ol_discount;
    public String ol_status;

    public OrderLine() {

    }

    public OrderLine setRandomValues(SeededRandomHelper seededRandomHelper) {
        ol_id = seededRandomHelper.getUUID().toString();
        o_id = null;
        i_id = null;
        ol_qty = seededRandomHelper.getIntBetween(1, 100);
        ol_discount = seededRandomHelper.getFloatBetween(0, 100);
        ol_status = seededRandomHelper.getStringWithLength(2, 16);
        return this;
    }

    public void fillStatement(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, ol_id);
        preparedStatement.setString(2, o_id);
        preparedStatement.setString(3, i_id);
        preparedStatement.setInt(4, ol_qty);
        preparedStatement.setFloat(5, ol_discount);
        preparedStatement.setString(6, ol_status);

    }

    public void initWithResultSet(ResultSet resultSet) throws SQLException {
        ol_id = resultSet.getString("ol_id");
        o_id = resultSet.getString("o_id");
        i_id = resultSet.getString("i_id");
        ol_qty = resultSet.getInt("ol_qty");
        ol_discount = resultSet.getFloat("ol_discount");
        ol_status = resultSet.getString("ol_status");
    }

    @Override
    public String getSQLInsertString() {
        return "INSERT INTO order_line (ol_id, o_id, i_id, ol_qty, ol_discount, ol_status) VALUES (?, ?, ?, ?, ?, ? )";
    }

    @Override
    public String getBasicSQLSelfSelectString() {
        return String.format("SELECT * FROM order_line WHERE ol_id = '%s'", ol_id);
    }

    @Override
    public String getBasicSQLAllSelectString() {
        return String.format("SELECT * FROM order_line");
    }

    @Override
    public AbstractMap.SimpleEntry<String, String> getPrimaryKeyNameAndValue() {
        return new AbstractMap.SimpleEntry<>("ol_id", ol_id);
    }

    @Override
    public String toString() {
        return "OrderLine{" +
                "ol_id='" + ol_id + '\'' +
                ", o_id='" + o_id + '\'' +
                ", i_id='" + i_id + '\'' +
                ", ol_qty=" + ol_qty +
                ", ol_discount=" + ol_discount +
                ", ol_status='" + ol_status + '\'' +
                '}';
    }
}
