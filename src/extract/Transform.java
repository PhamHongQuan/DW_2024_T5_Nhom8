package extract;

import model.DataFile;
import module.GetConnection;

import java.io.IOException;
import java.sql.*;

public class Transform {
    Connection warehouse = null;
    Connection conn = null;
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
    private DataFile getProcessWH(String status,String destination) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        DataFile dataFile = null;
//        DataFileConfig dataFileConfig = null;

        String query = "SELECT data_file.*,data_file_configs.* FROM `data_file` \n" +
                "JOIN data_file_configs ON data_file_configs.id = data_file.df_config_id\n" +
                "WHERE `status`=? AND destination=?";

            // Thực hiện truy vấn để lấy thông tin từ data_file_configs
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, status);
            preparedStatement.setString(2, destination);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {

                // Tạo đối tượng model.DataFileConfig và gán giá trị từ ResultSet
                dataFile = new DataFile();
                dataFile.setId(resultSet.getLong("id"));
                dataFile.setDfConfigId(resultSet.getInt("df_config_id"));
                dataFile.setName(resultSet.getString("name"));
                dataFile.setRowCount(resultSet.getInt("row_count"));
                dataFile.setStatus(resultSet.getString("status"));
                dataFile.setNote(resultSet.getString("note"));
                System.out.println("Co du liệu id config");

            }
            else {
                // 4.1 Thông báo lỗi
                new GetConnection().logFile("Lấy thông tin thất bại");
                System.exit(0);
            }
        return  dataFile;
    }

    private void updateStatus(int dataFileId, String newStatus,String note) throws SQLException, IOException {
        conn = new GetConnection().getConnection("control");
        String updateStatusAndErrorSql = "UPDATE data_file SET status = ?,note =? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, note);
            updateStatement.setLong(3, dataFileId);
            updateStatement.executeUpdate();
        }
    }
    private void updateStatusConfig(int dataFileConfigId, String des) throws SQLException {
        String updateStatusAndErrorSql = "UPDATE data_file_configs SET destination = ? WHERE id = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateStatusAndErrorSql)) {
            updateStatement.setString(1, des);
            updateStatement.setLong(2, dataFileConfigId);
            updateStatement.executeUpdate();
        }
    }
    public void truncateTable(Connection conn, String tableName) {
        try (Statement statement = conn.createStatement()) {
            String truncateQuery = "TRUNCATE TABLE " + tableName;
            statement.executeUpdate(truncateQuery);
            System.out.println("Đã truncate bảng " + tableName);
        } catch (SQLException e) {
            System.out.println("Lỗi khi thực hiện TRUNCATE TABLE: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean transformDataBankDim() throws IOException {
        Connection staging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.bank_dim (bank_name, dt_expired) " +
                "SELECT DISTINCT bank_name, '2030-01-01' as dt_expired " +
                "FROM staging.exchange_rate";

        // 7.1.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Thành công");

            // Trả về true nếu có ít nhất một hàng bị ảnh hưởng (được chèn)
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }

    public boolean transformDataCurrencyDim() throws IOException {
        Connection stagging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.currency_dim (id_bank, currency_code, currency_name, dt_expired)\n" +
                "SELECT\n" +
                "    bd.id_bank,\n" +
                "    er.currency_code,\n" +
                "    er.currency_name,\n" +
                "    '2030-01-01' as dt_expired\n" +
                "FROM staging.exchange_rate er\n" +
                "JOIN data_warehouse.bank_dim bd ON er.bank_name = bd.bank_name";

        //7.2.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }

    }
    public boolean transformDataDateDim() throws IOException {
        Connection stagging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.date_dim (date, day, month, year, hour, minute)\n" +
                "SELECT\n" +
                "    `date`,\n" +
                "    DAY(`date`) as day,\n" +
                "    MONTH(`date`) as month,\n" +
                "    YEAR(`date`) as year,\n" +
                "    HOUR(`date`) as hour,\n" +
                "    MINUTE(`date`) as minute\n" +
                "FROM staging.exchange_rate\n" +
                "GROUP BY `date`";

        // 7.3.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }
    public boolean transformDataExchangeRateFact() throws IOException {
        Connection stagging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.exchange_rate_fact (id_date, id_currency, buy_cash_rate, buy_transfer_rate, sale_rate, dt_expired)\n" +
                "SELECT\n" +
                "    dd.id_date,\n" +
                "    cd.id_currency,\n" +
                "    er.buy_cash_rate,\n" +
                "    er.buy_transfer_rate,\n" +
                "    er.sale_rate,\n" +
                "    '2030-01-01' as dt_expired -- Sử dụng ngày cố định thay vì NOW()\n" +
                "FROM staging.exchange_rate er\n" +
                "JOIN data_warehouse.date_dim dd ON er.date = dd.date\n" +
                "JOIN data_warehouse.currency_dim cd ON er.currency_code = cd.currency_code\n" +
                "ORDER BY er.date, cd.id_currency";

        // 7.4.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }
    public boolean transformDataAggregate() throws IOException {
        Connection stagging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.exchange_rate_aggregate (date, currency_code, currency_name, bank_name, buy_cash_rate, buy_transfer_rate, sale_rate, create_by, update_by, update_at)\n" +
                "SELECT\n" +
                "    dd.date,\n" +
                "    cd.currency_code,\n" +
                "    cd.currency_name,\n" +
                "    bd.bank_name,\n" +
                "    AVG(er.buy_cash_rate) as buy_cash_rate,\n" +
                "    AVG(er.buy_transfer_rate) as buy_transfer_rate,\n" +
                "    AVG(er.sale_rate) as sale_rate,\n" +
                "    'nhat' as create_by,\n" +
                "    'nhat' as update_by,\n" +
                "    NOW() as update_at\n" +
                "FROM staging.exchange_rate er\n" +
                "JOIN data_warehouse.date_dim dd ON er.date = dd.date\n" +
                "JOIN data_warehouse.currency_dim cd ON er.currency_code = cd.currency_code\n" +
                "JOIN data_warehouse.bank_dim bd ON er.bank_name = bd.bank_name\n" +
                "GROUP BY dd.date, cd.currency_code, bd.bank_name";

        // 7.5.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }
    public boolean transformDataAvg() throws IOException {
        Connection stagging = new GetConnection().getConnection("staging");
        warehouse = new GetConnection().getConnection("warehouse");

        // Transform dữ liệu vào bảng bank_dim
        String transformBankDimQuery = "INSERT INTO data_warehouse.avg_rate_aggregate (month_avg, year_avg, currency_code, currency_name, bank_name, avg_buy_cash_rate, avg_buy_transfer_rate, avg_sale_rate, create_by, update_by, update_at)\n" +
                "SELECT\n" +
                "    MONTHNAME(dd.date) as month_avg,\n" +
                "    YEAR(dd.date) as year_avg,\n" +
                "    cd.currency_code,\n" +
                "    cd.currency_name,\n" +
                "    bd.bank_name,\n" +
                "    AVG(er.buy_cash_rate) as avg_buy_cash_rate,\n" +
                "    AVG(er.buy_transfer_rate) as avg_buy_transfer_rate,\n" +
                "    AVG(er.sale_rate) as avg_sale_rate,\n" +
                "    'nhat' as create_by,\n" +
                "    'nhat' as update_by,\n" +
                "    NOW() as update_at\n" +
                "FROM staging.exchange_rate er\n" +
                "JOIN data_warehouse.date_dim dd ON er.date = dd.date\n" +
                "JOIN data_warehouse.currency_dim cd ON er.currency_code = cd.currency_code\n" +
                "JOIN data_warehouse.bank_dim bd ON er.bank_name = bd.bank_name\n" +
                "GROUP BY MONTH(dd.date), YEAR(dd.date), cd.currency_code, bd.bank_name";

        // 7.6.1 kiểm tra có thành công không
        try (PreparedStatement preparedStatement = warehouse.prepareStatement(transformBankDimQuery)) {
//            preparedStatement.setInt(1, dfConfigId);
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("thanh cong");
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.out.println("Thất bại");
            new GetConnection().logFile("Transform thất bại");
            System.exit(0);
            return false;
        }
    }
    public static void main(String[] args) throws SQLException, IOException {
        // 2.connect db
        Connection stagging = new GetConnection().getConnection("staging");
        Connection warehouse = new GetConnection().getConnection("warehouse");

        Transform t = new Transform();

        // 3. kiểm tra có tiến trình đang chạy
        if(t.checkProcessing("P","W")){
            System.out.println("Co tien trinh dang chay");
            // 3.1 Thông báo lỗi
            new GetConnection().logFile("Co tien trinh dang chay");
            System.exit(0);
        }
        else {
            System.out.println("Khong co tien trinh dang chay");

        }

        // 4.lấy thông tin của  tiến trình transform chưa chạy
        System.out.println(t.getProcessWH("C","S"));
        DataFile dataFile = t.getProcessWH("C","S");
        // 5. Cập nhật  trạng thái để chạy tiến trình
        t.updateStatus((int) dataFile.getId(),"P","Process data transform");
        t.updateStatusConfig(dataFile.getDfConfigId(),"W");

        // 6. Tiến hành truncate các bảng ở warehouse
        t.truncateTable(warehouse,"bank_dim");
        t.truncateTable(warehouse,"currency_dim");
        t.truncateTable(warehouse,"date_dim");
        t.truncateTable(warehouse,"exchange_rate_fact");
        t.truncateTable(warehouse,"exchange_rate_aggregate");
        t.truncateTable(warehouse,"avg_rate_aggregate");

        // 7. Tiến hành transform dữ liệu
        // 7.1 transform bảng bank_dim
        t.transformDataBankDim();
        // 7.2 transform bảng currency_dim
        t.transformDataCurrencyDim();
        // 7.3 transform bảng date_dim
        t.transformDataDateDim();
        // 7.4 transform bảng exchange_rate_fact
        t.transformDataExchangeRateFact();
        // 7.5 transform bảng exchange_rate_aggregate
        t.transformDataAggregate();
        // 7.6 transform bảng avg_rate_aggregate
        t.transformDataAvg();

        t.updateStatus((int) dataFile.getId(),"C","Transform data succesfull");
        t.truncateTable(stagging,"exchange_rate");
    }
}
