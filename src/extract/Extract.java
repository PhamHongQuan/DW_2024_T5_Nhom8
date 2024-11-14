package extract;

import model.DataFile;
import model.DataFileConfig;
import module.GetConnection;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extract {
    Connection conn = null;
    PreparedStatement pre_control = null;

    //Thêm dữ liệu mới vào bảng data_file và trả về đối tượng DataFile.
    public DataFile addDataFile(int dfConfigId,String sourceUrl) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String insertSql = "INSERT INTO data_file (df_config_id, name, row_count, status, note, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String currentDateTime = dateFormat.format(new Date());
        String csvFileName="";
        switch (sourceUrl){
            case "vietcombank.com" :
                csvFileName = "vietcombank_data_" + currentDateTime + ".csv";
                break;
            case "bidv.com" :
                csvFileName = "bidv_data_" + currentDateTime + ".csv";
                break;
        }
//        String csvFileName = "vietcombank_data_" + currentDateTime + ".csv";

        try (PreparedStatement preparedStatement = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            int dfConfig = dfConfigId;
            String name = csvFileName;
            int rowCount = 24;
            String status = "N";
            String note = "Data imported successfully";
            Date createdAt = new Date();
            Timestamp updatedAt = null;
            String createdBy = "Nghĩa";
            String updatedBy = null;

            preparedStatement.setLong(1, dfConfig);
            preparedStatement.setString(2, name);
            preparedStatement.setInt(3, rowCount);
            preparedStatement.setString(4, status);
            preparedStatement.setString(5, note);
            preparedStatement.setTimestamp(6, new Timestamp(createdAt.getTime()));
            preparedStatement.setTimestamp(7, new Timestamp(createdAt.getTime()));
            preparedStatement.setString(8, createdBy);
            preparedStatement.setString(9, updatedBy);

            preparedStatement.executeUpdate();

            // Retrieve the generated keys (including the ID of the inserted row)
            ResultSet resultSet = preparedStatement.getGeneratedKeys();

            if (resultSet.next()) {
                long generatedId = resultSet.getLong(1);

                // Create a DataFile object with the inserted data
                DataFile dataFile = new DataFile();
                dataFile.setId(generatedId);
                dataFile.setDfConfigId(dfConfigId);
                dataFile.setName(name);
                dataFile.setRowCount(rowCount);
                dataFile.setStatus(status);
                dataFile.setNote(note);

                return dataFile;
            } else {
                // Handle the case where no generated keys are available
                return null;
            }
        }
    }

    //Kiểm tra và lấy cấu hình DataFileConfig theo ID từ bảng data_file_configs
    public DataFileConfig check(int idFileConfig) throws IOException {
        conn = new GetConnection().getConnection("control");
        DataFileConfig dataFileConfig = null;
        // Câu SQL chèn dữ liệu vào bảng data_file
        String query = "SELECT data_file_configs.id, data_file_configs.source_path, data_file_configs.location,data_file_configs.format,data_file_configs.colums, data_file_configs.destination from data_file_configs " +
                "WHERE data_file_configs.id = ?";
        try {
            // Thực hiện truy vấn để lấy thông tin từ data_file_configs
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setInt(1, idFileConfig);
            ResultSet resultSet = preparedStatement.executeQuery();

            // Kiểm tra xem có dữ liệu trả về không
            if (resultSet.next()) {
                dataFileConfig = new DataFileConfig();
                dataFileConfig.setId(resultSet.getLong("id"));
                dataFileConfig.setSourcePath(resultSet.getString("source_path"));
                dataFileConfig.setLocation(resultSet.getString("location"));
                dataFileConfig.setFormat(resultSet.getString("format"));
                dataFileConfig.setColumns(resultSet.getString("colums"));
                dataFileConfig.setDestination(resultSet.getString("destination"));
//                System.out.println("Co du liệu id config");
            } else {
                new GetConnection().logFile("Không tìm thấy dữ liệu cho idFileConfig:");
                System.out.println("Không tìm thấy dữ liệu cho idFileConfig: " + idFileConfig);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataFileConfig;
    }

    // Chạy script Python để crawl dữ liệu từ nguồn, ghi log trạng thái.
    public static boolean runScript(String  urlSource) throws IOException {
        boolean success = false;
        File csvFile=null;
        switch (urlSource){
            // 9.1 vietcombank
            case "vietcombank.com" :
                // 9.1.1 run file "D:\\DW_2024_T5_Nhom8\\module\\crawl\\vcb_crawl.py"
                // 9.3 kiểm tra chạy thành công hay không
                if(new RunPythonScript().runScript("D:\\DW_2024_T5_Nhom8\\module\\crawl\\vcb_crawl.py")){
                    System.out.println("Chạy script data thành công");
                    success = true;
                    new GetConnection().logFile("Chạy script data thành công");
                }
                else {
                    System.out.println("Chạy script data không thành công");
                    success = false;
                    new GetConnection().logFile("Chạy script data không thành công");
                }
                break;
            // 9.1 bidv.com
            case "bidv.com" :
                // 9.2.1 run file "D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py"
                // 9.3 kiểm tra chạy thành công hay không
                if(new RunPythonScript().runScript("D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py")){
                    System.out.println("Chạy script data thành công");
                    success = true;
                    new GetConnection().logFile("Chạy script data thành công");
                }
                else {
                    System.out.println("Chạy script data không thành công");
                    success = false;
                    new GetConnection().logFile("Chạy script data không thành công");
                }
                break;
        }
        return success;
    }

    // Cập nhật trạng thái và ghi chú cho một bản ghi trong data_file
    private void updateStatus(int dataFileId, String newStatus,String note) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?,note =? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }

    // Cập nhật tên file cho một bản ghi trong data_file
    private void updateFileName(int dataFileId,String actualFileName) throws SQLException {
        String updateStatusAndFileNameSql = "UPDATE data_file SET name = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndFileNameSql)) {
            updateStatement.setString(1, actualFileName);
            updateStatement.setLong(2, dataFileId);
            updateStatement.executeUpdate();
        }
    }

    // Kiểm tra tiến trình đang chạy cho điểm đến cụ thể
    private boolean checkProcessing(String status,String destination) throws IOException {
        conn = new GetConnection().getConnection("control");
        String query = "SELECT `status`,destination FROM `data_file` \n" +
                "JOIN data_file_configs ON data_file_configs.id = data_file.df_config_id\n" +
                "WHERE `status`=? AND destination=?";
        try {
            // Thực hiện truy vấn để lấy thông tin từ data_file_configs
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, destination);
            ResultSet resultSet = preparedStatement.executeQuery();
            // Kiểm tra xem có bản ghi nào trả về hay không
            if (resultSet.next()) {
                // Có bản ghi, trả về true
                return true;
            } else {
                // Không có bản ghi, trả về false
                return false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  false;
    }

    // Thêm cấu hình mới vào bảng data_file_configs và trả về ID
    private static int addDataConfigFile(Connection connection,String source_url,String folder) throws SQLException {
        // Câu SQL chèn dữ liệu vào bảng data_file
        String insertSql = "INSERT INTO data_file_configs (description, source_path, location, format, `seperator`, colums, destination, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        long dfConfigId = -1;
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql,Statement.RETURN_GENERATED_KEYS)) {
            String source_path = source_url;
            String location = folder;
            Date createdAt = new Date(); // Lấy ngày hiện tại
            Timestamp updatedAt = null; // Chưa có thông tin về ngày cập nhật
            String createdBy = "Nghĩa";
            String updatedBy = null; // Chưa có thông tin về người cập nhật

            // Thay đổi dữ liệu này dựa trên cấu trúc bảng và cột thực tế của bạn
            preparedStatement.setString(1, "nguồn lấy dữ liệu"); // description
            preparedStatement.setString(2,source_path ); // source_path
            preparedStatement.setString(3,location ); // location
            preparedStatement.setString(4, ".csv"); // format
            preparedStatement.setString(5, ","); // separator
            preparedStatement.setString(6, "24"); // columns
            preparedStatement.setString(7, "F"); // destination
            preparedStatement.setTimestamp(8, new Timestamp(createdAt.getTime()));
            preparedStatement.setTimestamp(9,  null);
            preparedStatement.setString(10, createdBy); // created_by
            preparedStatement.setString(11, updatedBy); // updated_by

            // Thực hiện chèn dữ liệu
            int affectedRows = preparedStatement.executeUpdate();

            if (affectedRows > 0) {
                // Lấy ID được tạo tự động
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        dfConfigId = generatedKeys.getInt(1);
                    }
                }
            }
        }

        return (int) dfConfigId;
    }

    // Đọc file cấu hình, thêm các nguồn vào data_file_configs, lưu các ID để xử lý
    private List<Integer> loadConfig() throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String link = "D:\\DW_2024_T5_Nhom8\\module\\config\\config.properties";
        List<Integer> dfConfigIds = new ArrayList<>();

        InputStream input = null;
        try {
            Properties properties = new Properties();
            input = new FileInputStream(link);
            properties.load(input);

            // Lấy giá trị của khóa url_source
            String urlSourceValue = properties.getProperty("url_source");
            String urlForderLocation = properties.getProperty("folder_location");

            // Phân tách các giá trị theo dấu phẩy
            List<String> urlList = Arrays.asList(urlSourceValue.split(","));

            // In ra các giá trị
            for (String url : urlList) {
                System.out.println("URL: " + url);

                // 5. thêm source vào bảng data_file_configs
                int dfConfigId = addDataConfigFile(conn,url,urlForderLocation);
                System.out.println("Inserted data_file_configs row with ID: " + dfConfigId);
                dfConfigIds.add(dfConfigId);
            }
            System.out.println("urlForderLocation: " + urlForderLocation);
        } catch (IOException e) {
            // 5.1 kiểm tra nếu thêm thành công hay không
            // 5.1.1 Thông báo lỗi
            new GetConnection().logFile("Thêm config source thất bại");
        }

        return dfConfigIds;
    }

    public static void main(String[] args) throws SQLException, IOException {
        // 1. đọc file config.property
        // 2.connect db
        Connection connect = new GetConnection().getConnection("control");
        Extract n = new Extract();

        // 3. kiểm tra có tiến trình đang chạy
        if (n.checkProcessing("P", "F")) {
            System.out.println("Co tien trinh dang chay  ");
            new GetConnection().logFile("Co tien trinh dang chay");
            System.exit(0);
        } else {
            System.out.println("Khong co tien trinh dang chay  ");
        }

        // 4. load config source
        List<Integer> dfConfigIds = n.loadConfig();
        System.out.println(dfConfigIds);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (Integer dfConfigId : dfConfigIds) {
            executor.submit(() -> {
                DataFileConfig dataFileConfig = null;

                try {
                    // 5.1 kiểm tra nếu thêm thành công hay không
                    dataFileConfig = n.check(dfConfigId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println(dataFileConfig);

                int dfDataConfigId = (int) dataFileConfig.getId();

                DataFile dataFile = null;
                try {
                    // 6. Thêm dòng mới vào data_file dựa vào id của data_file_configs
                    // 6.1 Kiểm tra có thêm thành công
                    dataFile = n.addDataFile(dfDataConfigId,dataFileConfig.getSourcePath());
                    System.out.println(dataFile);
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }

                try {
                    // 7. updateStatus của data_file sang P
                    n.updateStatus((int) dataFile.getId(), "P", "Data import process");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
//
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                // 8. Chạy runScript với tham số là source
                boolean crawlSuccess = false;
                try {
                    crawlSuccess = runScript(dataFileConfig.getSourcePath());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                // 9. kiểm tra source
                // 9.3 kiểm tra chạy thành công hay không
                if (crawlSuccess) {
                    System.out.println("Crawl operation successful.");
                    try {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
                        String currentDateTime = dateFormat.format(new Date());

                        String csvFileName = "";
                        switch (dataFileConfig.getSourcePath()) {
                            case "vietcombank.com":
                                csvFileName = "vietcombank_data_" + currentDateTime + ".csv";
                                break;
                            case "bidv.com":
                                csvFileName = "bidv_data_" + currentDateTime + ".csv";
                                break;
                        }
                        // 9.5 cập nhật data_file thành C
                        n.updateStatus((int) dataFile.getId(), "C", "Data import success");
                        // 9.6 cập nhât lại tên file
                        n.updateFileName((int) dataFile.getId(), csvFileName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Crawl operation failed.");
                    try {
                        new GetConnection().logFile("Crawl operation failed.");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        // 9.4 cập nhật data_file thành E
                        n.updateStatus((int) dataFile.getId(), "E", "Data import error");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        // 10. Dừng luồng
        executor.shutdown();
    }
}
