package cc.hiifong.bigdata;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author hiifong
 * @Date 2023/6/9 10:45
 * @Email i@hiif.ong
 */
public class HiveExternalTable {
    static String driverName = "org.apache.hive.jdbc.HiveDriver";
    static String url = "jdbc:hive2://master:10000/default";
    static String username = "root";
    static String password = "h";
    static Connection connection;
    static Statement statement;

    static String SQL = "create external table if not exists student(\n" +
            "    name string,\n" +
            "    course string,\n" +
            "    score int\n" +
            ")\n" +
            "row format delimited fields terminated by \",\" location \"/output/\"";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        connection = DriverManager.getConnection(url, username, password);
        statement = connection.createStatement();
        statement.execute(SQL);
        connection.close();
    }
}
