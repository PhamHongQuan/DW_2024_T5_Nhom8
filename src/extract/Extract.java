package extract;

import model.DataFile;
import model.DataFileConfig;
import module.GetConnection;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extract {
    Connection conn = null;

    // Tạo tên file CSV dựa trên nguồn dữ liệu
    private static String createCSVFileName(String sourcePath) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String currentDateTime = dateFormat.format(new Date());
        switch (sourcePath) {
            case "vietcombank.com":
                return "vietcombank_data_" + currentDateTime + ".csv";
            case "bidv.com":
                return "bidv_data_" + currentDateTime + ".csv";
            default:
                throw new IllegalStateException("Source path không hợp lệ: " + sourcePath);
        }
    }

    // Kiểm tra xem file đã từng được crawl chưa
    private boolean isFileAlreadyCrawled(String fileName, String inputDate) throws SQLException, IOException, ParseException {
        conn = new GetConnection().getConnection("control");

        // Chuyển đổi ngày sang định dạng chuẩn
        String formattedDate = new SimpleDateFormat("yyyyMMdd").format(new SimpleDateFormat("dd/MM/yyyy").parse(inputDate));

        // Tạo tên file
        String generatedFileName = fileName.split("\\.")[0] + "_data_" + formattedDate + ".csv";

        // Truy vấn kiểm tra trong cơ sở dữ liệu
        String query = "SELECT COUNT(*) AS file_count FROM data_file WHERE name = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, generatedFileName);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next() && resultSet.getInt("file_count") > 0; // Trả về true nếu file đã tồn tại
        }
    }


    // Thêm dữ liệu mới vào bảng data_file
    public DataFile addDataFile(int dfConfigId, String sourceUrl, String inputDate) throws SQLException, IOException, ParseException {
        conn = new GetConnection().getConnection("control");
        String insertSql = "INSERT INTO data_file (df_config_id, name, row_count, status, note, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // Chuyển đổi ngày sang định dạng chuẩn
        SimpleDateFormat inputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = inputDateFormat.parse(inputDate);
        String formattedDate = outputDateFormat.format(date);

        // Tạo tên file
        String csvFileName = "";
        switch (sourceUrl) {
            case "vietcombank.com":
                csvFileName = "vietcombank_data_" + formattedDate + ".csv";
                break;
            case "bidv.com":
                csvFileName = "bidv_data_" + formattedDate + ".csv";
                break;
        }

        // Thực hiện thêm vào cơ sở dữ liệu
        try (PreparedStatement preparedStatement = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, dfConfigId);
            preparedStatement.setString(2, csvFileName);
            preparedStatement.setInt(3, 24);
            preparedStatement.setString(4, "N");
            preparedStatement.setString(5, "Data imported successfully");
            preparedStatement.setTimestamp(6, new Timestamp(new Date().getTime()));
            preparedStatement.setTimestamp(7, new Timestamp(new Date().getTime()));
            preparedStatement.setString(8, "Nghĩa");
            preparedStatement.setString(9, null);

            preparedStatement.executeUpdate();

            ResultSet resultSet = preparedStatement.getGeneratedKeys();

            if (resultSet.next()) {
                long generatedId = resultSet.getLong(1);
                DataFile dataFile = new DataFile();
                dataFile.setId(generatedId);
                dataFile.setDfConfigId(dfConfigId);
                dataFile.setName(csvFileName);
                dataFile.setRowCount(24);
                dataFile.setStatus("N");
                dataFile.setNote("Data imported successfully");
                System.out.println("Crawl dữ liệu hoàn tất cho file: " + dataFile.getName());
                return dataFile;
            } else {
                System.out.println("Crawl dữ liệu cho file không thành công");
                return null;
            }
        }
    }

    // Kiểm tra và lấy cấu hình DataFileConfig theo ID
    public DataFileConfig loadDataConfig(int idFileConfig) throws IOException {
        conn = new GetConnection().getConnection("control");
        DataFileConfig dataFileConfig = null;
        String query = "SELECT * FROM data_file_configs WHERE id = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setInt(1, idFileConfig);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                dataFileConfig = new DataFileConfig();
                dataFileConfig.setId(resultSet.getLong("id"));
                dataFileConfig.setSourcePath(resultSet.getString("source_path"));
                dataFileConfig.setLocation(resultSet.getString("location"));
                dataFileConfig.setFormat(resultSet.getString("format"));
                dataFileConfig.setColumns(resultSet.getString("colums"));
                dataFileConfig.setDestination(resultSet.getString("destination"));
            } else {
                return null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataFileConfig;
    }

    public static boolean runScript(String urlSource, String inputDate) throws IOException {
        boolean success = false;
        switch (urlSource) {
            // 7.1
            case "vietcombank.com":
                // 7.1.1
                if (new RunPythonScript().runScriptPy("D:\\DW_2024_T5_Nhom8\\module\\crawl\\vcb_crawl.py", inputDate)) {
                    System.out.println("Chạy script data thành công");
                    success = true;
                } else {
                    // 7.1.2
                    System.out.println("Chạy script data không thành công");
                }
                break;
            // 7.2
            case "bidv.com":
                // 7.2.1
                if (new RunPythonScript().runScriptPy("D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py", inputDate)) {
                    System.out.println("Chạy script data thành công");
                    success = true;
                } else {
                    // 7.2.2
                    System.out.println("Chạy script data không thành công");
                }
                break;
        }
        return success;
    }

    private void updateStatus(int dataFileId, String newStatus, String note) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?, note = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }

    private boolean checkProcessing(String status, String destination) throws IOException {
        conn = new GetConnection().getConnection("control");
        String query = "SELECT `status`, destination FROM `data_file` " +
                "JOIN data_file_configs ON data_file_configs.id = data_file.df_config_id " +
                "WHERE `status` = ? AND destination = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, destination);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private List<Integer> loadConfig() throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String link = "D:\\DW_2024_T5_Nhom8\\module\\config\\config.properties";
        List<Integer> dfConfigIds = new ArrayList<>();
        try (InputStream input = new FileInputStream(link)) {
            Properties properties = new Properties();
            properties.load(input);
            String urlSourceValue = properties.getProperty("url_source");
            List<String> urlList = Arrays.asList(urlSourceValue.split(","));
            for (String url : urlList) {
                int dfConfigId = getDataFileConfigId(url);
                if (dfConfigId != -1) {
                    dfConfigIds.add(dfConfigId);
                }
            }
        } catch (IOException e) {
            new GetConnection().logFile("Không tìm thấy config source");
        }
        return dfConfigIds;
    }

    private int getDataFileConfigId(String sourceUrl) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String query = "SELECT id FROM data_file_configs WHERE source_path = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, sourceUrl);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            }
        }
        return -1;
    }

    public static void main(String[] args) throws SQLException, IOException {

        String inputDate = "22/11/2024";

        // Chuẩn bị kết nối và cấu hình
        Extract ex = new Extract();

        // Bước 1: Đọc file config.property
        // Bước 2: Kết nối database (thực hiện tự động trong loadConfig và GetConnection)
        List<Integer> configIds = ex.loadConfig();

        // 3. Kiểm tra có tiến trình đang chạy hay không
        if (ex.checkProcessing("P", "F")) {
            System.out.println("Có tiến trình đang chạy");
            System.exit(0);
        }


        //  Khởi tạo ExecutorService để xử lý song song
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (Integer configId : configIds) {
            executor.submit(() -> {
                try {
                    // Bước 4: Load config source và xử lý từng cấu hình
                    // Lấy thông tin cấu hình dữ liệu
                    DataFileConfig dataFileConfig = ex.loadDataConfig(configId);

                    // 5. Kiểm tra đã có dữ liệu trong data_file_config chưa
                    if (dataFileConfig == null) {
                        // 5.1 Thông báo chưa có dữ liệu trong data_file_config
                        System.out.println("Không có dữ liệu trong bảng data_file_config");
                        new GetConnection().logFile("Không tìm thấy dữ liệu cho idFileConfig:");
                        return;
                    }

                    // Bước 6: Kiểm tra file đã được crawl chưa
                    if (ex.isFileAlreadyCrawled(dataFileConfig.getSourcePath(), inputDate)) {
                        // 6.1 Thông báo đã từng được crawl rồi
                        System.out.println("File đã được crawl trước đó: " + dataFileConfig.getSourcePath());
                        return;
                    }

                    // Bước 7: Chạy script để crawl dữ liệu
                    boolean crawl = Extract.runScript(dataFileConfig.getSourcePath(), inputDate);

                    // Bước 8: Kiểm tra trạng thái crawl thành công hay không
                    if (crawl) {
                        // 9. Thông báo thành công và Thêm dòng mới vào data_file dựa vào id của data_file_configs
                        System.out.println("Crawl dữ liệu thành công.");
                        DataFile newDataFile = ex.addDataFile(configId, dataFileConfig.getSourcePath(), inputDate);
                        // 10 Cập nhật trạng thái data_file sang "P"
                        ex.updateStatus((int) newDataFile.getId(), "P", "Processing");

                        // 11. Kiểm tra xem đối tượng newDataFile được tạo thành công không
                        if (newDataFile != null) {
                            String csvFileName = createCSVFileName(dataFileConfig.getSourcePath());
                            // 11.2 Cập nhật trạng thái file thành "C" khi hoàn thành
                            ex.updateStatus((int) newDataFile.getId(), "C", "Data import success");
                        }else {
                            // 11.1 Thông báo Chưa thêm được đối tượng mới vào bảng data_file
                            System.out.println("Chưa thêm được đối tượng mới vào bảng data_file");
                        }
                    } else {
                        // 8.1 Thông báo crawl thất bại
                        System.out.println("Crawl operation failed.");
                        // Cập nhật trạng thái file thành E và ghi log
                        new GetConnection().logFile("Crawl operation failed for source: " + dataFileConfig.getSourcePath());
                        ex.updateStatus((int) dataFileConfig.getId(), "E", "Data import error");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Bước 12: Dừng executor sau khi hoàn thành
        executor.shutdown();
    }
}
