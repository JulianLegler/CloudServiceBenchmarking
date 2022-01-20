package berlin.tu.csb.model;

import berlin.tu.csb.controller.SeededRandomHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface DatabaseTableModel {
    DatabaseTableModel setRandomValues(SeededRandomHelper seededRandomHelper);

    void fillStatement(PreparedStatement preparedStatement) throws SQLException;

    void initWithResultSet(ResultSet resultSet) throws SQLException;

    String getSQLInsertString();

    String getBasicSQLSelfSelectString();
}
