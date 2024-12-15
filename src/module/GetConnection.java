package module;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class GetConnection {
    String driver = null;
    String url = null;
    String user = null;
    String pass = null;
    String databasebName = null;
    boolean checkE = false;

    public boolean getCheckE() {
        return checkE;
    }

    public void setCheckE(boolean check) {
        checkE = check;
    }

    public boolean updateError(String location, String message) throws IOException {
        if (location.equalsIgnoreCase("control")) {
            // 3.2 log file
            logFile(message);
            checkE = false;
            System.exit(0);
        } else {
            // chuyển trạng thái -> đổi update = true
            checkE = true;
        }
        return checkE;
    }

    public void logFile(String message) throws IOException {
        FileWriter fw = new FileWriter("D:\\DW_2024_T5_Nhom8\\file\\logs\\logs.txt", true);
        PrintWriter pw = new PrintWriter(fw);
        pw.println(message + "\t");
        pw.println("HH:mm:ss dd/MM/yyyy - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")));
        pw.println("-----");
        pw.close();
    }

    public Connection getConnection(String location) throws IOException {
        String link = "D:\\DW_2024_T5_Nhom8\\module\\config\\config.properties";
        Connection result = null;

        // 3. ket noi db control
        if (location.equalsIgnoreCase("control")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);
                // lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databasebName = prop.getProperty("dbName_control");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");
            } catch (IOException ex) {
                //  8.3.1, 3.1 Thông báo không tìm thấy file
                System.out.println("Unknown file " + link);
                // Log file
                logFile("Unknown file " + link + "\n" + ex.getMessage());
                System.exit(0);
            }
            // 8.3 ket noi db staging
        } else if (location.equalsIgnoreCase("staging")) {
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);
                // lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databasebName = prop.getProperty("dbName_staging");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");
            } catch (IOException ex) {
                //  8.3.1, 3.1 Thông báo không tìm thấy file
                System.out.println("Unknown file" + link);
                // Log file
                logFile("Unknown file " + link + "\n" + ex.getMessage());
                System.exit(0);
            }
        }
        else if(location.equalsIgnoreCase("warehouse")){
            try (InputStream input = new FileInputStream(link)) {
                Properties prop = new Properties();
                prop.load(input);
                // 2.2.1 lấy từng thuộc tính cấu hình trong file config
                driver = prop.getProperty("driver_local");
                url = prop.getProperty("url_local");
                databasebName = prop.getProperty("dName_datawarehouse");
                user = prop.getProperty("user_local");
                pass = prop.getProperty("pass_local");
                System.out.println("ket noi thanh cong");
            } catch (IOException ex) {
                System.out.println("ket noi ko thanh cong");
                System.out.println("Unknown file " + link);
                // Log file
                logFile("Unknown file " + link + "\n" + ex.getMessage());
                System.exit(0);
            }
        }
        try {
            // đăng kí driver
            Class.forName(driver);
            String connectionURL = url + databasebName;
            try {
                // kết nối
                result = DriverManager.getConnection(connectionURL, user, pass);
            } catch (SQLException e) {
                // 8.3.1, 3.1 thông báo lỗi kết nối
                System.out.println("Error connect " + location);
                updateError(location, "Error connect " + location + "\n" + e.getLocalizedMessage());
            }
        } catch (ClassNotFoundException e) {
            //  8.3.1, 3.1 thông báo không đăng ký được driver
            System.out.println("Driver not connect");
            logFile("driver not connect" + "\n" + e.getMessage());
            System.exit(0);
        }
        return result;
    }


        public static void main(String[] args) {
            // Tạo một đối tượng GetConnection
            GetConnection connectionManager = new GetConnection();

            // Kiểm tra kết nối đến "control"
            try {
                Connection connection = connectionManager.getConnection("control");
                if (connection != null) {
                    System.out.println("Kết nối thành công tới 'control'.");
                    connection.close(); // Đóng kết nối sau khi kiểm tra
                } else {
                    System.out.println("Kết nối không thành công tới 'control'.");
                }
            } catch (IOException e) {
                System.out.println("Lỗi trong quá trình xử lý file cấu hình: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Lỗi không xác định: " + e.getMessage());
            }
        }

}
