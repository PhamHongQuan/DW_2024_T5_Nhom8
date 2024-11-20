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
    PreparedStatement pre_control = null;

    private boolean isFileAlreadyCrawled(String fileName, String inputDate) throws SQLException, IOException, ParseException {
        conn = new GetConnection().getConnection("control");

        // Chuyển đổi ngày từ "dd/MM/yyyy" thành "yyyyMMdd"
        String formattedDate = new SimpleDateFormat("yyyyMMdd").format(new SimpleDateFormat("dd/MM/yyyy").parse(inputDate));

        // Tạo tên file
        String generatedFileName = fileName.split("\\.")[0] + "_data_" + formattedDate + ".csv";

        // Kiểm tra xem tên file có tồn tại trong cơ sở dữ liệu không
        String query = "SELECT COUNT(*) AS file_count FROM data_file WHERE name = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, generatedFileName);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next() && resultSet.getInt("file_count") > 0;  // Trả về true nếu file đã tồn tại
        }
    }


    // Thêm dữ liệu mới vào bảng data_file và trả về đối tượng DataFile.
    public DataFile addDataFile(int dfConfigId, String sourceUrl, String inputDate) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String insertSql = "INSERT INTO data_file (df_config_id, name, row_count, status, note, created_at, update_at, create_by, update_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // Chuyển đổi inputDate từ "dd/MM/yyyy" sang "yyyyMMdd"
        SimpleDateFormat inputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            date = inputDateFormat.parse(inputDate);  // Parse inputDate vào kiểu Date
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String formattedDate = outputDateFormat.format(date);  // Định dạng lại ngày

        String csvFileName = "";
        switch (sourceUrl) {
            case "vietcombank.com":
                csvFileName = "vietcombank_data_" + formattedDate + ".csv";
                break;
            case "bidv.com":
                csvFileName = "bidv_data_" + formattedDate + ".csv";
                break;
        }

        try (PreparedStatement preparedStatement = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, dfConfigId);  // df_config_id
            preparedStatement.setString(2, csvFileName);  // Tên file với ngày đã chuyển đổi
            preparedStatement.setInt(3, 24);  // row_count
            preparedStatement.setString(4, "N");  // status
            preparedStatement.setString(5, "Data imported successfully");  // note
            preparedStatement.setTimestamp(6, new Timestamp(new Date().getTime()));  // created_at
            preparedStatement.setTimestamp(7, new Timestamp(new Date().getTime()));  // update_at
            preparedStatement.setString(8, "Nghĩa");  // create_by
            preparedStatement.setString(9, null);  // update_by (null)

            preparedStatement.executeUpdate();

            ResultSet resultSet = preparedStatement.getGeneratedKeys();

            if (resultSet.next()) {
                long generatedId = resultSet.getLong(1);
                DataFile dataFile = new DataFile();
                dataFile.setId(generatedId);
                dataFile.setDfConfigId(dfConfigId);
                dataFile.setName(csvFileName);  // Gán tên file vào đối tượng DataFile
                dataFile.setRowCount(24);
                dataFile.setStatus("N");
                dataFile.setNote("Data imported successfully");

                return dataFile;
            } else {
                return null;
            }
        }
    }

    // Kiểm tra và lấy cấu hình DataFileConfig theo ID từ bảng data_file_configs
    public DataFileConfig check(int idFileConfig) throws IOException {
        conn = new GetConnection().getConnection("control");
        DataFileConfig dataFileConfig = null;
        String query = "SELECT data_file_configs.id, data_file_configs.source_path, data_file_configs.location,data_file_configs.format,data_file_configs.colums, data_file_configs.destination FROM data_file_configs " +
                "WHERE data_file_configs.id = ?";
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
                new GetConnection().logFile("Không tìm thấy dữ liệu cho idFileConfig:");
                System.out.println("Không tìm thấy dữ liệu cho idFileConfig: " + idFileConfig);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataFileConfig;
    }

    public static boolean runScript(String urlSource, String inputDate) throws IOException {
        boolean success = false;
        switch (urlSource) {
            case "vietcombank.com":
                if (new RunPythonScript().runScript("D:\\DW_2024_T5_Nhom8\\module\\crawl\\vcb_crawl.py", inputDate)) {
                    System.out.println("Chạy script data thành công");
                    success = true;
                } else {
                    System.out.println("Chạy script data không thành công");
                }
                break;
            case "bidv.com":
                if (new RunPythonScript().runScript("D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py", inputDate)) {
                    System.out.println("Chạy script data thành công");
                    success = true;
                } else {
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

    private void updateFileName(int dataFileId, String actualFileName) throws SQLException {
        String updateStatusAndFileNameSql = "UPDATE data_file SET name = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndFileNameSql)) {
            updateStatement.setString(1, actualFileName);
            updateStatement.setLong(2, dataFileId);
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
        Connection connect = new GetConnection().getConnection("control");
        Extract n = new Extract();
        String inputDate = "01/11/2024";

        if (n.checkProcessing("P", "F")) {
            System.out.println("Co tien trinh dang chay");
            System.exit(0);
        }

        List<Integer> dfConfigIds = n.loadConfig();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (Integer dfConfigId : dfConfigIds) {
            executor.submit(() -> {
                DataFileConfig dataFileConfig = null;
                try {
                    dataFileConfig = n.check(dfConfigId);

                    if (dataFileConfig != null) {
                        if (n.isFileAlreadyCrawled(dataFileConfig.getSourcePath(), inputDate)) {
                            System.out.println("File đã được crawl trước đó: " + dataFileConfig.getSourcePath());
                            return;
                        }
                        boolean runScriptStatus = Extract.runScript(dataFileConfig.getSourcePath(), inputDate);

                        if (runScriptStatus) {
                            DataFile dataFile = n.addDataFile(dfConfigId, dataFileConfig.getSourcePath(), inputDate);  // Truyền thêm inputDate
                            if (dataFile != null) {
                                n.updateStatus((int) dataFile.getId(), "P", null);  // Cập nhật trạng thái thành "P" (Processing)
                                System.out.println("Crawl dữ liệu thành công cho config ID: " + dfConfigId);
                            }
                        }
                    }
                } catch (IOException | SQLException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executor.shutdown();
    }
}
