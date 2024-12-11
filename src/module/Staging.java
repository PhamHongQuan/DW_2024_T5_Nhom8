package module;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Staging {
    String url_source = null;
    String[] url_sources = null;
    boolean isTruncated = false;
    public static void main(String[] args) throws IOException {
        Staging staging = new Staging();
        staging.staging();
    }

    public void staging() throws IOException {
        System.out.println("start staging");
        Connection conn = null;
        PreparedStatement pre_control = null;
        String link = "D:\\DW\\DW_2024_T5_Nhom8\\module\\config\\config.properties";
        // 1. Đọc file config.properties
        try {
            InputStream input = new FileInputStream(link);
            Properties prop = new Properties();
            prop.load(input);
            // 2. Lấy đường dẫn các source data từ file config
            url_source = prop.getProperty("url_source");
            url_sources = url_source.trim().split(",");
            // 3. Kết nối db control
            conn = new GetConnection().getConnection("control");
            if (conn == null) {
                // 3.1. Thông báo lỗi nếu kết nối DB control không thành công
                System.out.println("Error connnect control. ");
                // 3.2. Ghi log với nội dung "Error connnect control"
                new GetConnection().logFile("Error connnect control .");
                System.exit(0);
            }
            // 4. Còn source data từ file config
            for (String us : url_sources) {
                try {
                    // 5. Tìm các hàng có status P và destination là S (tiến trình đang chạy)
                    ResultSet re = checkStatus(conn, pre_control, "P", "S", us);
                    // 6. Kiểm tra có dòng tiến trình đang chạy không
                    if (re.next()) {
                        // 6.1 Thông báo
                        System.out.println("Currently, there is another process at work.");
                    } else {
                        // 7. Tìm các hàng có status C và destination là F
                        re = checkStatus(conn, pre_control, "C", "F", us);
                        int id;
                        String filename = null;
                        // 8. Kiểm tra còn dòng không (Lấy giá trị từng dòng)
                        while (re.next()) {
                            id = re.getInt("id");
                            filename = re.getString("name");
                            int row_count = re.getInt("row_count");
                            String location = re.getString("location");
                            String path = location + "\\"+filename;
                            File file = new File(path);
                            String sql4 = "UPDATE data_file SET status='E', "
                                    + "data_file.update_at=now() WHERE id=" + id;
                            String sql3 = "UPDATE data_file join data_file_configs on data_file.df_config_id = data_file_configs.id " +
                                    "SET status='P', destination = 'S', data_file.update_at=now(), data_file_configs.update_at=now()" +
                                    " WHERE data_file.id="+ id;
                            // 8.1 Cập nhật trạng thái status='P' và destination = S
                            pre_control = conn.prepareStatement(sql3);
                            pre_control.executeUpdate();
                            // 8.2 Ktra file có tồn tại trong folder
                            if (!file.exists()) {
                                // file không tồn tại - cập nhật status: E - thông báo
                                pre_control = conn.prepareStatement(sql4);
                                // 8.2.1 cập nhật status: Error
                                pre_control.executeUpdate();
                                // 8.2.2 thông báo file không tồn tại
                                System.out.println(path + " does not exist");
                            } else {
                                // file tồn tại - kết nối db staging - load dữ liệu - thông báo thành công -
                                // cập nhật status: C, destination: S và status: E khi không thể load all data
                                // 8.3 kết nối db staging
                                GetConnection getConn = new GetConnection();
                                Connection conn_Staging = getConn.getConnection("staging");
                                // 8.3.1 Thông báo "Error connect staging"
                                if (getConn.getCheckE()) {
                                    getConn.setCheckE(false);
                                    pre_control.close();
                                    // 8.3.2 Cập nhật trạng thái E
                                    pre_control = conn.prepareStatement(sql4);
                                    pre_control.executeUpdate();
                                    System.exit(0);
                                } else {
                                    // 8.3.3 Xóa toàn bộ dữ liệu cũ
                                    if(!isTruncated) {
                                        String stagingTable = "exchange_rate";
                                        String truncateSql = "TRUNCATE TABLE " + stagingTable;
                                        PreparedStatement preTruncate = conn_Staging.prepareStatement(truncateSql);
                                        preTruncate.executeUpdate();
                                        // 8.3.4 đặt lại giá trị của biến isTruncated là true
                                        isTruncated = true;
                                    }
                                }


                                // 8.4 import data từ file vào db staging
                                int count = 0;
                                String sql = "LOAD DATA LOCAL INFILE '" + path.replace("\\", "\\\\") + "' " +
                                        "INTO TABLE exchange_rate " +
                                        "FIELDS TERMINATED BY ',' " +
                                        "OPTIONALLY ENCLOSED BY '\"' " +
                                        "IGNORE 1 LINES;";
                                PreparedStatement pre_Staging = conn_Staging.prepareStatement(sql);
                                try {
                                    count = pre_Staging.executeUpdate();
                                } catch (Exception e) {
                                    // 8.4.1 Thông báo "The file is not in the correct format"
                                    System.out.println("The file is not in the correct format");
                                }
                                // 8.5 Kiểm tra có load hết dữ liệu không
                                if (count == row_count) {
                                    String sql2 = "UPDATE data_file join data_file_configs on data_file.df_config_id = data_file_configs.id " +
                                            "SET status='C', destination = 'S', data_file.update_at=now() WHERE data_file.id=" + id;
                                    pre_control.close();
                                    pre_control = conn.prepareStatement(sql2);
                                    // 8.5.1 cập nhật status: C, destination: S
                                    pre_control.executeUpdate();
                                } else {
                                    String sql2 = "UPDATE data_file SET status='E', data_file.update_at=now() " +
                                            "WHERE id="+ id;
                                    pre_control.close();
                                    pre_control = conn.prepareStatement(sql2);
                                    // 8.5.2 cập nhật status: E
                                    pre_control.executeUpdate();
                                }
                                // 8.6 thông báo hoàn thành
                                System.out.println("Complete:\n" + "file name: " + filename
                                        + " ,total: " + count + "/" + row_count + " row");
                            }
                        }
                    }
                    re.close();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
            if (pre_control != null) {
                pre_control.close();
            }
            // 9. Đóng kết nối db
            conn.close();
        } catch (IOException ex) {
            // 1.1 Thông báo không tìm thấy file
            System.out.println("Unknown file " + link);
            // 1.2 Log file
            new GetConnection().logFile("Unknown file " + link + "\n" + ex.getMessage());
            System.exit(0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("the end staging");
    }

    public ResultSet checkStatus(Connection conn, PreparedStatement pre_control, String status, String destination, String bank) throws SQLException {
        pre_control = conn.prepareStatement("SELECT data_file.id, data_file.name, data_file.row_count," +
                        "data_file_configs.source_path, data_file_configs.location," +
                        "data_file_configs.format,data_file_configs.colums, data_file_configs.destination " +
                        "from data_file JOIN data_file_configs ON data_file.df_config_id = data_file_configs.id " +
                        "where data_file.status='" + status + "' AND data_file_configs.destination='" + destination
                        + "' AND data_file_configs.source_path='" + bank + "'");
        return pre_control.executeQuery();
    }


}
