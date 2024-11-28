package mart;

import com.mysql.jdbc.ConnectionImpl;
import module.GetConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Mart {
    public static void Mart() throws SQLException, IOException {
        Connection conn_control = null;
        PreparedStatement pre_control = null;

        conn_control = new GetConnection().getConnection("control");

        try {
            result = DriverManager.getConnection(conn_control, user, pass);
            checkE = 1;
        } catch (SQLException e) {

            if (location.equalsIgnoreCase( anotherString: "control")) {
                System.out.println("Kết nối không thành công");
                Timestamp date = new Timestamp (System.currentTimeMillis());
                String date_err = date.toString();
                String fileName = "D:\\logs\\LogERR-"date_err.replaceAll( regex: "\\s", replacement: "").replace(
                        PrintWriter writer = new PrintWriter(new FileWriter (fileName, append: true));
                writer.println("Error: " + e.getMessage());
                e.printStackTrace(writer);
                writer.close();
                System.exit(status: 0);
            }
        }


        public static ResultSet checkStatus (Connection conn, PreparedStatement pre_control, String status, String destination) throws SQLException
                pre_control = conn.prepareStatement (sql: "SELECT data_file.id, data_file.name, data_file.row_count," + "data_file_configs.source_path, data_file_configs.location,"
                + "data_file_configs. format, data_file_configs.colums, data_file_configs.destination
                + "from data_file JOIN data_file_configs ON data_file.df_config_id = data_file_configs.id "
                + "where data_file.status="status" AND data_file_configs.destination=" + destination +
                + "and data_file.update_at < CURRENT_TIMESTAMP ORDER BY data_file.update_at DESC LIMIT 2");
            return pre_control.executeQuery();
        }



    }
}
