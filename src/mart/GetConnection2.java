package mart;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

public class GetConnection2 {
    String driver = null;
    String url = null;
    String user = null;
    String pass = null;
    String databasebName = null;
    public static int checkE;

    public Connection getConnection(String location) throws IOException, SQLException {
        // Đường dẫn đến file cấu hình
        String filePath = "config.properties";

        // Sử dụng ClassLoader để đọc tệp tin
        ClassLoader classLoader = GetConnection2.class.getClassLoader();
        Connection result = null;

        try (InputStream input = classLoader.getResourceAsStream(filePath)) {
            if (input == null) {
                throw new IOException("Không thể tìm thấy file cấu hình.");
            }

            Properties prop = new Properties();
            prop.load(input);

            // Đọc cấu hình dựa trên location
            switch (location.toLowerCase()) {
                case "warehouse":
                    driver = prop.getProperty("driver_local");
                    url = prop.getProperty("url_local");
                    databasebName = prop.getProperty("dbName_datawarehouse");
                    user = prop.getProperty("user_local");
                    pass = prop.getProperty("pass_local");
                    break;

                case "mart":
                    driver = prop.getProperty("driver_local");
                    url = prop.getProperty("url_local");
                    databasebName = prop.getProperty("dbName_datamart");
                    user = prop.getProperty("user_local");
                    pass = prop.getProperty("pass_local");
                    break;

                case "control":
                    driver = prop.getProperty("driver_local");
                    url = prop.getProperty("url_local");
                    databasebName = prop.getProperty("dbName_control");
                    user = prop.getProperty("user_local");
                    pass = prop.getProperty("pass_local");
                    break;

                default:
                    throw new IllegalArgumentException("Location không hợp lệ: " + location);
            }
        } catch (IOException | IllegalArgumentException e) {
            logError(e.getMessage());
            throw new IOException("Lỗi khi đọc cấu hình hoặc vị trí không hợp lệ.", e);
        }

        // Kết nối tới cơ sở dữ liệu
        try {
            // Đăng ký driver JDBC
            Class.forName(driver);

            String connectionURL = url + databasebName;
            result = DriverManager.getConnection(connectionURL, user, pass);
            checkE = 1;
        } catch (ClassNotFoundException | SQLException e) {
            logError(e.getMessage());
            checkE = 0;
            throw new SQLException("Không thể kết nối đến cơ sở dữ liệu.", e);
        }

        return result;
    }

    // Hàm ghi lỗi vào file log
    private void logError(String errorMessage) {
        Timestamp date = new Timestamp(System.currentTimeMillis());
        String date_err = date.toString().replaceAll("\\s", "").replace(":", "-");
        String fileName = "D:\\DW\\DW_2024_T5_Nhom8\\file\\logs\\logERR-" + date_err + ".txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) {
            writer.println("Error: " + errorMessage);
        } catch (IOException e) {
            System.out.println("Lỗi khi ghi log: " + e.getMessage());
        }
    }
}
