package berlin.tu.csb.model;

import berlin.tu.csb.controller.SeededRandomHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.UUID;

public class Item implements DatabaseTableModel{
    public String i_id;
    public String i_title;
    public Timestamp i_pub_date;
    public String i_publisher;
    public String i_subject;
    public String i_desc;
    public float i_srp;
    public float i_cost;
    public String i_isbn;
    public int i_page;

    public Item() {

    }

    public DatabaseTableModel setRandomValues(SeededRandomHelper seededRandomHelper) {
        i_id = seededRandomHelper.getUUID().toString();
        i_title = seededRandomHelper.getStringWithLength(5, 60);
        i_pub_date = new Timestamp(seededRandomHelper.getLongBetween(1, System.currentTimeMillis())); //LocalDateTime.of(RandomUtils.nextInt(1900, 2022), RandomUtils.nextInt(1, 12), RandomUtils.nextInt(1, 30), 0, 0, 0);
        i_publisher = seededRandomHelper.getStringWithLength(5, 60);
        i_subject = seededRandomHelper.getStringWithLength(5, 60);
        i_desc = seededRandomHelper.getStringWithLength(60, 500);
        i_srp = seededRandomHelper.getFloatBetween(5, 120);
        i_cost = i_srp*seededRandomHelper.getFloatBetween(0.5f, 1);
        i_isbn = seededRandomHelper.getStringWithLength(12, 13);
        i_page = seededRandomHelper.getIntBetween(10, 1800);
        return this;
    }

    public void fillStatement(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, i_id);
        preparedStatement.setString(2, i_title);
        preparedStatement.setTimestamp(3, i_pub_date);
        preparedStatement.setString(4, i_publisher);
        preparedStatement.setString(5, i_subject);
        preparedStatement.setString(6, i_desc);
        preparedStatement.setFloat(7, i_srp);
        preparedStatement.setFloat(8, i_cost);
        preparedStatement.setString(9, i_isbn);
        preparedStatement.setInt(10, i_page);
    }

    public void initWithResultSet(ResultSet resultSet) throws SQLException {
        i_id = resultSet.getString("i_id");
        i_title = resultSet.getString("i_title");
        i_pub_date = resultSet.getTimestamp("i_pub_date");
        i_publisher = resultSet.getString("i_publisher");
        i_subject = resultSet.getString("i_subject");
        i_desc = resultSet.getString("i_desc");
        i_srp = resultSet.getFloat("i_srp");
        i_cost = resultSet.getFloat("i_cost");
        i_isbn = resultSet.getString("i_isbn");
        i_page = resultSet.getInt("i_page");
    }

    @Override
    public String getSQLInsertString() {
        return "INSERT INTO item (i_id, i_title, i_pub_date, i_publisher, i_subject, i_desc, i_srp, i_cost, i_isbn, i_page) VALUES (?, ?, ?, ?, ?, ? ,?, ?, ?, ?)";
    }

    @Override
    public String getBasicSQLSelfSelectString() {
        return String.format("SELECT * FROM item WHERE i_id = '%s'", i_id);
    }

    @Override
    public String getBasicSQLAllSelectString() {
        return String.format("SELECT * FROM item");
    }

    @Override
    public AbstractMap.SimpleEntry<String, String> getPrimaryKeyNameAndValue() {
        return new AbstractMap.SimpleEntry<>("i_id", i_id);
    }

    @Override
    public String toString() {
        return "Item{" +
                "i_id='" + i_id + '\'' +
                ", i_title='" + i_title + '\'' +
                ", i_pub_date=" + i_pub_date +
                ", i_publisher='" + i_publisher + '\'' +
                ", i_subject='" + i_subject + '\'' +
                ", i_desc='" + i_desc + '\'' +
                ", i_srp=" + i_srp +
                ", i_cost=" + i_cost +
                ", i_isbn='" + i_isbn + '\'' +
                ", i_page=" + i_page +
                '}';
    }
}
