package berlin.tu.csb;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

public class Item {
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

    public Item setRandomValues() {
        i_id = UUID.randomUUID().toString();
        i_title = RandomStringUtils.randomAlphanumeric(5, 60);
        i_pub_date = new Timestamp(RandomUtils.nextLong(1, System.currentTimeMillis())); //LocalDateTime.of(RandomUtils.nextInt(1900, 2022), RandomUtils.nextInt(1, 12), RandomUtils.nextInt(1, 30), 0, 0, 0);
        i_publisher = RandomStringUtils.randomAlphanumeric(5, 60);
        i_subject = RandomStringUtils.randomAlphanumeric(5, 60);
        i_desc = RandomStringUtils.randomAlphanumeric(60, 500);
        i_srp = RandomUtils.nextFloat(5f, 120f);
        i_cost = i_srp*RandomUtils.nextFloat(0.5f, 1f);
        i_isbn = RandomStringUtils.randomAlphanumeric(13);
        i_page = RandomUtils.nextInt(10, 1800);
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
