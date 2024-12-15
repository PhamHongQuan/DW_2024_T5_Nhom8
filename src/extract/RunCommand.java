package extract;

import model.DataFile;
import model.DataFileConfig;
import module.GetConnection;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RunCommand {
    Connection conn = null;
    PreparedStatement pre_control = null;
    CrawlVietcombank vietcom;
    public DataFile addDataFile(int dfConfigId, String sourceUrl) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String insertSql = "INSERT INTO data_file (df_config_id, name, row_count, status, note, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
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
            String createdBy = "Nhật";
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

                // Tạo đối tượng model.DataFileConfig và gán giá trị từ ResultSet
                dataFileConfig = new DataFileConfig();
                dataFileConfig.setId(resultSet.getLong("id"));
                dataFileConfig.setSourcePath(resultSet.getString("source_path"));
                dataFileConfig.setLocation(resultSet.getString("location"));
                dataFileConfig.setFormat(resultSet.getString("format"));
                dataFileConfig.setColumns(resultSet.getString("colums"));
                dataFileConfig.setDestination(resultSet.getString("destination"));

                System.out.println("Co du liệu id config");

                // CrawlData.crawl(sourcePath, location, format, columns, destination);
            } else {
                System.out.println("Không tìm thấy dữ liệu cho idFileConfig: " + idFileConfig);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataFileConfig;
    }
    public static boolean runScript(String  urlSource){
        boolean success = false;
        File csvFile=null;
        switch (urlSource){
            case "vietcombank.com" :
                if(new RunPythonScript().runScript("D:\\DW_2024_T5_Nhom8\\module\\crawl\\vietxml.py")){
                    System.out.println("Chạy script data thành công");
                    success = true;
                }
                else {
                    System.out.println("Chạy script data không thành công");
                    success = false;
                }
                break;
            case "bidv.com" :
                if(new RunPythonScript().runScript("D:\\DW_2023_T5_Nhom8\\module\\crawl\\bidv_crawl.py")){
                    System.out.println("Chạy script data thành công");
                    success = true;
                }
                else {
                    System.out.println("Chạy script data không thành công");
                    success = false;
                }
                break;
        }
        return success;
    }
    private void updateStatus(int dataFileId, String newStatus,String note) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?,note =? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }
    private void updateFileName(int dataFileId,String actualFileName) throws SQLException {
        String updateStatusAndFileNameSql = "UPDATE data_file SET name = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndFileNameSql)) {
            updateStatement.setString(1, actualFileName);
            updateStatement.setLong(2, dataFileId);
            updateStatement.executeUpdate();
        }
    }
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
            String createdBy = "Nhật";
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
    private List<Integer> loadConfig(String urlSource, String folderLocation) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String link = "D:\\DW_2024_T5_Nhom8\\module\\config\\config.properties";
        List<Integer> dfConfigIds = new ArrayList<>();
        File configFile = new File(link);

        if (!configFile.exists()) {
            System.out.println("Config file does not exist: " + link);
            System.exit(0);
        }
        else {
            System.out.println("file da ton tai");
        }

        InputStream input = null;
        try {
            Properties properties = new Properties();
            input = new FileInputStream(link);
            properties.load(input);

            // Lấy giá trị của khóa url_source
            String urlSourceValue = properties.getProperty("url_source");
            String urlForderLocation = properties.getProperty("folder_location");

            // Phân tách các giá trị theo dấu phẩy
            List<String> urlList = Arrays.asList(urlSource.split(","));

            // In ra các giá trị
            for (String url : urlList) {
                System.out.println("URL: " + url);

                int dfConfigId = addDataConfigFile(conn,url,urlForderLocation);
                System.out.println("Inserted data_file_configs row with ID: " + dfConfigId);
                dfConfigIds.add(dfConfigId);
            }
            System.out.println("urlForderLocation: " + urlForderLocation);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return dfConfigIds;
    }
    public static void main(String[] args) throws SQLException, IOException {
        if (args.length < 2) {
            System.out.println("Usage: java Main <urlSource> <folderLocation>");
            System.exit(1);
        }

        for (String arg : args) {
            System.out.println("Argument: " + arg);
        }

        String specificSource = args[0].trim();
        String folderLocation = args[1].trim();

        Connection connect = new GetConnection().getConnection("control");
        RunCommand n = new RunCommand();

        if (n.checkProcessing("P", "F")) {
            System.out.println("Co tien trinh dang chay  ");
        } else {
            System.out.println("Khong co tien trinh dang chay  ");
        }

        List<Integer> dfConfigIds = n.loadConfig(specificSource, folderLocation);
        System.out.println(dfConfigIds);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (Integer dfConfigId : dfConfigIds) {
            executor.submit(() -> {
                DataFileConfig dataFileConfig = null;
                try {
                    dataFileConfig = n.check(dfConfigId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(dataFileConfig);

                if (dataFileConfig.getSourcePath().equalsIgnoreCase(specificSource)) {
                    // Only process the specified source
                    int dfDataConfigId = (int) dataFileConfig.getId();

                    DataFile dataFile = null;
                    try {
                        dataFile = n.addDataFile(dfDataConfigId, dataFileConfig.getSourcePath());
                        System.out.println(dataFile);
                    } catch (SQLException | IOException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        n.updateStatus((int) dataFile.getId(), "P", "Data import process");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                    boolean crawlSuccess = runScript(dataFileConfig.getSourcePath());

                    if (crawlSuccess) {
                        System.out.println("Crawl operation successful.");
                        try {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
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
                            n.updateStatus((int) dataFile.getId(), "C", "Data import success");
                            n.updateFileName((int) dataFile.getId(), csvFileName);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("Crawl operation failed.");
                        try {
                            n.updateStatus((int) dataFile.getId(), "E", "Data import error");
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        executor.shutdown();
    }

}
