//package file;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.sql.*;
//import java.util.ArrayList;
//
//public class Mart {
//	static Timestamp date = new Timestamp(System.currentTimeMillis());
//	static String date_err = date.toString();
//	static String fileName = "D:\\LogERR" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";
//
//    //	3. Truy vấn dữ liệu có trường là STATUS  từ bảng data_file_configs với điều kiện
////	(data_file.update_at < CURRENT_TIMESTAMP )
//    public static ResultSet checkStatus(Connection conn, PreparedStatement pre_control, String status,
//                                        String destination) throws SQLException {
//        pre_control = conn.prepareStatement("SELECT data_file.id, data_file.name, data_file.row_count,"
//                + "data_file_configs.source_path, data_file_configs.location,"
//                + "data_file_configs.format,data_file_configs.colums, data_file_configs.destination "
//                + "from data_file JOIN data_file_configs ON data_file.df_config_id = data_file_configs.id "
//                + "where data_file.status='" + status + "' AND data_file_configs.destination='" + destination + "'"
//                + "and data_file.update_at < CURRENT_TIMESTAMP ORDER BY data_file.update_at DESC LIMIT 2");
//        return pre_control.executeQuery();
//    }
//
//    public static void insertWhToMartAvg(Connection conn_wh, Connection conn_mart, PreparedStatement pre_control,
//                                         Connection conn_control, ArrayList<Integer> idArr) throws SQLException, IOException {
//
//        String selectQuery = "SELECT * FROM avg_rate_aggregate where update_at < CURRENT_TIMESTAMP ORDER BY update_at DESC limit 2";
//        try (PreparedStatement selectStatement = conn_wh.prepareStatement(selectQuery);
//             ResultSet resultSet = selectStatement.executeQuery()) {
//
//            // Thực hiện truy vấn INSERT ở mart
//            String insertQuery = "INSERT INTO mart.db_rename_avg (month_avg, year_avg, currency_code, currency_name, bank_name, avg_buy_cash_rate, avg_buy_transfer_rate, avg_sale_rate, create_at, update_at, create_by, update_by)"
//                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//            try (PreparedStatement insertStatement = conn_mart.prepareStatement(insertQuery)) {
//                while (resultSet.next()) {
//                    // Lấy giá trị từ kết quả SELECT
//                    String month_avg = resultSet.getString("month_avg");
//                    String year_avg = resultSet.getString("year_avg");
//                    String currencyCode = resultSet.getString("currency_code");
//                    String currency_name = resultSet.getString("currency_name");
//                    String bank_name = resultSet.getString("bank_name");
//                    Float avg_buy_cash_rate = resultSet.getFloat("avg_buy_cash_rate");
//                    Float avg_buy_transfer_rate = resultSet.getFloat("avg_buy_transfer_rate");
//                    Float avg_sale_rate = resultSet.getFloat("avg_sale_rate");
//                    String create_at = resultSet.getString("create_at");
//                    Timestamp update_at = new Timestamp(System.currentTimeMillis());
//                    String create_by = resultSet.getString("create_by");
//                    String update_by = resultSet.getString("update_by"); // ... (Lấy giá trị từ các cột
//                    // khác)
//
//                    // Thiết lập giá trị trong truy vấn INSERT
//                    insertStatement.setString(1, month_avg);
//                    insertStatement.setString(2, year_avg);
//                    insertStatement.setString(3, currencyCode);
//                    insertStatement.setString(4, currency_name);
//                    insertStatement.setString(5, bank_name);
//                    insertStatement.setFloat(6, avg_buy_cash_rate);
//                    insertStatement.setFloat(7, avg_buy_transfer_rate);
//                    insertStatement.setFloat(8, avg_sale_rate);
//                    insertStatement.setString(9, create_at);
//                    insertStatement.setTimestamp(10, update_at);
//                    insertStatement.setString(11, create_by);
//                    insertStatement.setString(12, update_by);
//
//                    // ... (Thiết lập giá trị cho các cột khác)
//
//                    // Thực hiện truy vấn INSERT
//                    insertStatement.executeUpdate();
//
//                }
//                System.out.println("Insert avg thành công");
//            } catch (Exception e) {
////				PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
////				writer.println("Error: " + e.getMessage());
////	            e.printStackTrace(writer);
////
////	            // Đóng file sau khi ghi xong
////	            writer.close();
//                updateErrSQL(pre_control, conn_control, idArr, "insert exchange", e);
//            }
//        }
//    }
//
//    public static void insertWhToMartExchange(Connection conn_wh, Connection conn_mart, PreparedStatement pre_control,
//                                              Connection conn_control, ArrayList<Integer> idArr) throws SQLException, IOException {
//
//        String selectQuery = "SELECT * FROM exchange_rate_aggregate where update_at < CURRENT_TIMESTAMP ORDER BY update_at DESC limit 43";
//        try (PreparedStatement selectStatement = conn_wh.prepareStatement(selectQuery);
//             ResultSet resultSet = selectStatement.executeQuery()) {
//
//            // Thực hiện truy vấn INSERT ở mart
//            String insertQuery = "INSERT INTO mart.db_rename_exchange (date, currency_code, currency_name, bank_name, buy_cash_rate, buy_transfer_rate, sale_rate, create_at, update_at, create_by, update_by)"
//                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//            try (PreparedStatement insertStatement = conn_mart.prepareStatement(insertQuery)) {
//                while (resultSet.next()) {
//                    // Lấy giá trị từ kết quả SELECT
//                    String date = resultSet.getString("date");
//                    String currencyCode = resultSet.getString("currency_code");
//                    String currency_name = resultSet.getString("currency_name");
//                    String bank_name = resultSet.getString("bank_name");
//                    Float buy_cash_rate = resultSet.getFloat("buy_cash_rate");
//                    Float buy_transfer_rate = resultSet.getFloat("buy_transfer_rate");
//                    Float sale_rate = resultSet.getFloat("sale_rate");
//                    String create_at = resultSet.getString("create_at");
//                    Timestamp update_at = new Timestamp(System.currentTimeMillis());
//                    String create_by = resultSet.getString("create_by");
//                    String update_by = resultSet.getString("update_by");
//
//                    // ... (Lấy giá trị từ các cột khác)
//
//                    // Thiết lập giá trị trong truy vấn INSERT
//                    insertStatement.setString(1, date);
//                    insertStatement.setString(2, currencyCode);
//                    insertStatement.setString(3, currency_name);
//                    insertStatement.setString(4, bank_name);
//                    insertStatement.setFloat(5, buy_cash_rate);
//                    insertStatement.setFloat(6, buy_transfer_rate);
//                    insertStatement.setFloat(7, sale_rate);
//                    insertStatement.setString(8, create_at);
//                    insertStatement.setTimestamp(9, update_at);
//                    insertStatement.setString(10, create_by);
//                    insertStatement.setString(11, update_by);
//
//                    // ... (Thiết lập giá trị cho các cột khác)
//
//                    // Thực hiện truy vấn INSERT
//                    insertStatement.executeUpdate();
//
//                }
//                System.out.println("Insert exchange thành công");
//            } catch (Exception e) {
//                updateErrSQL(pre_control, conn_control, idArr, "insert exchange", e);
//            }
//        }
//    }
//
//    public static void updateErrConnect(PreparedStatement pre_control, Connection conn_control, ArrayList<Integer> idArr, String mess)
//            throws IOException {
//        try {
//            if (GetConnection2.checkE == 0) {
//                for (Integer idE : idArr) {
//                    String sqlE = "UPDATE data_file SET status='E', data_file.update_at=now() WHERE id=" + idE;
//                    pre_control = conn_control.prepareStatement(sqlE);
//                    pre_control.executeUpdate();
//                }
//                System.out.println("Kết nối đến " + mess + " không thành công");
//                System.exit(0);
//            }
//        } catch (Exception e) {
//            PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
//            writer.println("Error: " + e.getMessage());
//            e.printStackTrace(writer);
//            writer.close();
//            System.exit(0);
//            // TODO Auto-generated catch block
//        }
//    }
//
//    public static void updateErrSQL(PreparedStatement pre_control, Connection conn_control, ArrayList<Integer> idArr, String mess,
//                                    Exception e) throws SQLException, IOException {
//        System.out.println("Lỗi khi " + mess + " không thành công");
//        for (Integer idE : idArr) {
//            String sqlE = "UPDATE data_file SET status='E', data_file.update_at=now() WHERE id=" + idE;
//            pre_control = conn_control.prepareStatement(sqlE);
//            pre_control.executeUpdate();
//        }
//
//        PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
//        writer.println("Error: " + e.getMessage());
//        e.printStackTrace(writer);
//
//        // Đóng file sau khi ghi xong
//        writer.close();
//
//        System.exit(0);
//    }
//
//    public static void Mart() throws SQLException, IOException {
//
//        Connection conn_control = null;
//        PreparedStatement pre_control = null;
//
////		1. Sử dụng hàm getConnection2(location) để đọc file config.properties với location = control
////		2.
//		conn_control = new GetConnection2().getConnection("contol");
//		try {
////		3.
////		4. Kiểm tra status = "P" && destination = "M" (Kiểm tra có tiến trình đang chạy không)
//            ResultSet re1 = checkStatus(conn_control, pre_control, "P", "M");
//            if (re1.next()) {
//                System.out.println("Tồn tại tiến trình đang chạy.");
//                System.exit(0);
//            } else {
//
////		5. Kiểm tra status = "C" && destination = "W"
//                ResultSet re2 = checkStatus(conn_control, pre_control, "C", "W");
//                if (!re2.next()) {
//                    System.out.println("Không có File ở warehouse");
////			5.1 Đóng kết nối dbControl
//                    conn_control.close();
//                    System.exit(0);
//                }
//
////		6. Cập nhật status = "P" và destination = "M" ở dòng vừa tìm đươc [Control.data_file_configs &&Control.data_file]
//                ArrayList<Integer> idArr = new ArrayList<>();
//                do {
//                    int id = re2.getInt("id");
//                    idArr.add(id);
//                    String sqlProcess = "UPDATE data_file SET status='P', data_file.update_at=now() WHERE id=" + id;
//                    pre_control = conn_control.prepareStatement(sqlProcess);
//                    pre_control.executeUpdate();
//
//                    String sqlProcess2 = "UPDATE data_file_configs SET destination = 'M', data_file_configs.update_at=now() WHERE id="
//                            + id;
//                    pre_control = conn_control.prepareStatement(sqlProcess2);
//                    pre_control.executeUpdate();
//                } while (re2.next());
//                // int id = re2.getInt("id");
//                // String sqlProcess = "UPDATE data_file SET status='P',
//                // data_file.update_at=now() WHERE id=" + id;
//                // pre_control = conn_control.prepareStatement(sqlProcess);
//                // pre_control.executeUpdate();
//
//                // String sqlProcess2 = "UPDATE data_file_configs SET destination = 'M',
//                // data_file_configs.update_at=now() WHERE id="
//                // + id;
//                // pre_control = conn_control.prepareStatement(sqlProcess2);
//                // pre_control.executeUpdate();
//
////		7. Sử dụng hàm getConnection2(location) để đọc file config.properties với location = warehouse
////		8. Kết nối DB warehouse
//                Connection conn_wh = new GetConnection2().getConnection("warehouse");
////			8.1  Tạo file ghi lỗi và update status ="E" của Control.data_file tại các dòng tìm được ở mục 6
//                updateErrConnect(pre_control, conn_control, idArr, "warehouse");
//
////		9. Sử dụng hàm getConnection2(location) để đọc file config.properties với location = mart
////		10. Kết nối server DB mart.
//                Connection conn_mart = new GetConnection2().getConnection("mart");
////			10.1  Tạo file ghi lỗi và update status ="E" của Control.data_file tại các dòng tìm được ở mục 6
//                updateErrConnect(pre_control, conn_control, idArr, "mart");
//
////		11. Sử dụng db mart
//                String useDatabaseSQL = "USE mart";
//                Statement useDatabaseStatement = conn_mart.createStatement();
//                useDatabaseStatement.execute(useDatabaseSQL);
//
////		12. Truncate 2 table rename
//                String truncateTabledb_rename_avg = "TRUNCATE TABLE db_rename_avg";
//                String truncateTabledb_rename_exchange = "TRUNCATE TABLE db_rename_exchange";
//                Statement statement = conn_mart.createStatement();
//                statement.executeUpdate(truncateTabledb_rename_avg);
//                statement.executeUpdate(truncateTabledb_rename_exchange);
//                System.out.println("Bảng truncateTable db_rename_exchange đã được truncate thành công.");
//                System.out.println("Bảng truncateTable db_rename_avg đã được truncate thành công.");
//
////		13. Copy data từ bảng exchange_rate_aggregate, avg_rate_aggregate vào bảng
////		db_rename_exchange, db_rename_avg bằng Procedure copyDB
//                String procedureCopy = "copyDB";
//                CallableStatement callableStatement = conn_mart.prepareCall("{call " + procedureCopy + "()}");
//                callableStatement.execute();
//                System.out.println("Stored procedure copy data đã được chạy thành công.");
//
////		14. Đọc dữ liệu từ bảng aggerate ở warehouse ghi vào mart
////		insertWhToMartAvg(connection_warehouse, connection_mart)
////		insertWhToMartExchange(connection_warehouse, connection_mart)
//                insertWhToMartAvg(conn_wh, conn_mart, pre_control, conn_control, idArr);
//                insertWhToMartExchange(conn_wh, conn_mart, pre_control, conn_control, idArr);
//
////		15. Rename 2 table bằng procedure renameTable
//                String procedureRename = "renameTable";
//                CallableStatement callableStatementRN = conn_mart.prepareCall("{call " + procedureRename + "()}");
//                callableStatementRN.execute();
//                System.out.println("Stored procedure rename table đã được chạy thành công.");
//
////		16. Đóng kết nối db mart
//                conn_mart.close();
//
////		17. Đóng kết nối db warehouse
//                conn_wh.close();
//                for (Integer idE : idArr) {
//
////		18. Update status = "C"  [Control.data_file_configs &&Control.data_file]
//                    String sqlProcess1 = "UPDATE data_file SET status='C', data_file.update_at=now() WHERE id=" + idE;
//
//                    pre_control = conn_control.prepareStatement(sqlProcess1);
//                    pre_control.executeUpdate();
//                }
////				}
//            }
//
////		18. Thông báo chạy thành công
//            System.out.println("Chạy thành công");
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
//            Timestamp date = new Timestamp(System.currentTimeMillis());
//            String date_err = date.toString();
//            String fileName = "D:\\logs\\LogERR" + date_err.replaceAll("\\s", "").replace(":", "-") + ".txt";
//
//            PrintWriter writer = new PrintWriter(new FileWriter(fileName, true));
//            writer.println("Error: " + e.getMessage());
//            e.printStackTrace(writer);
//
//            // Đóng file sau khi ghi xong
//            writer.close();
//            System.exit(0);
//        }
//
////		19. Đóng kết nối db Control
//        conn_control.close();
//    }
//
//    public static void main(String[] args) throws Exception {
//        Mart();
//    }
//}
