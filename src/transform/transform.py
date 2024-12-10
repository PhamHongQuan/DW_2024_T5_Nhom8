import pymysql
import configparser
from datetime import datetime
import sys
import os

#hàm ghi log
def write_log(error_name, error_description):
    # Đường dẫn thư mục log
    log_dir = "D:/DW_2024_T5_Nhom8/log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Tạo tên tệp log theo định dạng
    timestamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")[:-3]
    log_file_name = f"logERR-{timestamp}.txt"
    log_file_path = os.path.join(log_dir, log_file_name)

    # Nội dung log
    log_content = f"Tên lỗi: {error_name}\nMô tả lỗi: {error_description}\nThời gian lỗi: {timestamp}"

    # Ghi nội dung vào tệp log
    with open(log_file_path, "w", encoding="utf-8") as log_file:
        log_file.write(log_content)

    print(f"Log đã được ghi vào {log_file_path}")
    

try:
    config = configparser.ConfigParser()
    # 1. Đọc file config.properties
    config.read('D:\DW_2024_T5_NHOM8\config.properties')

    DB_CONFIG = {
        'host': config.get('DB', 'db_host'),
        'user': config.get('DB', 'db_user'),
        'password': config.get('DB', 'db_password'),
        'port': config.getint('DB', 'db_port'),
        'staging_db': config.get('DB', 'staging_db_name'),
        'warehouse_db': config.get('DB', 'warehouse_db_name'),
        'control_db': config.get('DB', 'control_db_name')
    }
    # 1.1. Thông báo lỗi đọc file config
except (configparser.Error, ValueError, KeyError) as e:
    # Ghi log lỗi nếu có vấn đề với file config
    error_name = "Config File Error"
    error_description = str(e)
    write_log(error_name, error_description)
    print(f"Lỗi khi đọc file config: {error_description}")
    
    
class DatabaseConnection:
    # Lớp này chịu trách nhiệm tạo kết nối đến database dựa trên tên database (staging, warehouse, control)
    @staticmethod
    def get_connection(db_name):
        return pymysql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG[db_name],
            port=DB_CONFIG['port']
        )

 # Lớp này chứa các phương thức để thực hiện kiểm tra, truy vấn, cập nhật dữ liệu và transform dữ liệu
class Transform:
    def __init__(self):
        try:
            # 2. Kết nối đến DB Staging, Warehouse, và Control
            self.conn_control = DatabaseConnection.get_connection('control_db')
            self.conn_staging = DatabaseConnection.get_connection('staging_db')
            self.conn_warehouse = DatabaseConnection.get_connection('warehouse_db')
        except Exception as e:
            # 2.1. Thông báo lỗi kết nối DB
            write_log("Database Connection Error", str(e))
            print(f"Lỗi khi kết nối đến cơ sở dữ liệu: {str(e)}")

    def check_processing(self, status, destination):
        query = """
        SELECT `status`, destination FROM `data_file`
        JOIN `data_file_configs` ON data_file_configs.id = data_file.df_config_id
        WHERE `status` = %s AND destination = %s
        """
        with self.conn_control.cursor() as cursor:
            cursor.execute(query, (status, destination))
            return cursor.fetchone() is not None

    def get_process_wh(self, name, status, destination):
        query = """
        SELECT data_file.*, data_file_configs.* FROM `data_file`
        JOIN `data_file_configs` ON data_file_configs.id = data_file.df_config_id
        WHERE `status` = %s AND destination = %s AND data_file.name = %s
        """
        with self.conn_control.cursor() as cursor:
            cursor.execute(query, (status, destination, name))
            result = cursor.fetchone()
            if result:
                return {
                    "id": result[0],
                    "df_config_id": result[1],
                    "name": result[2],
                    "row_count": result[3],
                    "status": result[4],
                    "note": result[5]
                }
            else:
                # 4.1. Thông báo lỗi không lấy được thông tin tiến trình
                raise ValueError(f"Không lấy được thông tin tiến trình với name = {name}.")

    # hàm update status cho control.data_file
    def update_status(self, data_file_id, new_status, note):
        query = "UPDATE `data_file` SET `status` = %s, `note` = %s WHERE `id` = %s"
        with self.conn_control.cursor() as cursor:
            cursor.execute(query, (new_status, note, data_file_id))
            self.conn_control.commit()

    def update_destination(self, process_id):
        # Cập nhật destination thành 'W' trong bảng control.data_file_configs
        update_query = """
        UPDATE control.data_file_configs
        SET destination = 'W'
        WHERE id = %s
        """
        with self.conn_staging.cursor() as cursor:
            cursor.execute(update_query, (process_id,))
            self.conn_staging.commit()

        print(f"Successfully updated destination for process_id {process_id} to 'W'.")
        
    # def isSame_bankname
    def isSame_bankname(self):
        query = """
        SELECT COUNT(1)
        FROM staging.exchange_rate AS er
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.bank_dim AS bd
            WHERE er.bank_name = bd.bank_name
        )
        """
        with self.conn_staging.cursor() as cursor:
            cursor.execute(query)
            # Nếu COUNT(1) = 0 thì tất cả bank_name đã tồn tại
            result = cursor.fetchone()
            return result[0] == 0

    
    # Hàm kiểm tra xem dữ liệu của cột currency_code và curreny.name có giống nhau ở hai bảng hay không (trả về true or false)
    # def isSame_currency
    def isSame_currencycode(self):
        query = """
        SELECT COUNT(1)
        FROM staging.exchange_rate AS er
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.currency_dim AS cd
            WHERE er.currency_code = cd.currency_code
        )
        """
        with self.conn_staging.cursor() as cursor:
            cursor.execute(query)
            # Nếu COUNT(1) = 0 thì tất cả currency_code đã tồn tại
            result = cursor.fetchone()
            return result[0] == 0
    
    def transform_table(self, conn, query):
        # Thực hiện truy vấn transform dữ liệu và trả về True nếu thành công
        with conn.cursor() as cursor:
            rows_affected = cursor.execute(query)
            conn.commit()
        return rows_affected > 0
    
    #transform data
    def transform_data(self):
   
        queries = []
        # 7.1. Kiểm tra dữ liệu cột bank_name của hai bảng staging.exchange_rate 
        # và warehouse.bank_dim giống nhau không
        if not self.isSame_bankname():
            # 7.1.1.Transform bảng warehouse.bank_dim
            queries.append({
                "description": "Kiểm tra và cập nhật dữ liệu bank_name",
                "query": """
                INSERT INTO warehouse.bank_dim (bank_name)
                SELECT DISTINCT er.bank_name
                FROM staging.exchange_rate AS er
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM warehouse.bank_dim AS bd
                    WHERE er.bank_name = bd.bank_name
                )
                """
            })

        # 7.2. Kiểm tra dữ liệu của cột curency_code và currency_name của hai bảng staging.exhange_rate 
        # và warehouse.currency_dim giống nhau không
        if not self.isSame_currencycode():
            # 7.2.1 Transform bảng warehouse.curreny_dim
            queries.append({
                "description": "Kiểm tra và cập nhật dữ liệu currency_code",
                 "query": """
                INSERT INTO warehouse.currency_dim (id_bank, currency_code, currency_name)
                SELECT DISTINCT 
                    bd.id_bank,
                    er.currency_code, 
                    er.currency_name
                FROM staging.exchange_rate AS er
                LEFT JOIN warehouse.bank_dim AS bd ON er.bank_name = bd.bank_name
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM warehouse.currency_dim AS cd
                    WHERE er.currency_code = cd.currency_code
                ) AND bd.id_bank IS NOT NULL
                """
            })

        # 7.3. Transform bảng date_dim
        queries.append({
            "description": "Transform date_dim",
            "query": """
           INSERT INTO date_dim (date, day, month, year)
           SELECT DISTINCT
                STR_TO_DATE(date, '%d/%m/%Y') AS normalized_date,
                DAY(STR_TO_DATE(date, '%d/%m/%Y')) AS day,
                MONTH(STR_TO_DATE(date, '%d/%m/%Y')) AS month,
                YEAR(STR_TO_DATE(date, '%d/%m/%Y')) AS year
            FROM staging.exchange_rate
            WHERE STR_TO_DATE(date, '%d/%m/%Y') IS NOT NULL;
            """
        })

        # 7.4. Transform bảng exchange_rate_fact
        queries.append({
            "description": "Transform exchange_rate_fact",
            "query": """
            INSERT INTO exchange_rate_fact (id_date, id_currency, id_bank, buy_cash_rate, buy_transfer_rate, sale_rate, dt_expired)
            SELECT
                dd.id_date,
                cd.id_currency,
                cd.id_bank,
                CAST(er.buy_cash_rate AS FLOAT),
                CAST(er.buy_transfer_rate AS FLOAT),
                CAST(er.sale_rate AS FLOAT),
                NULL
            FROM staging.exchange_rate er
            JOIN date_dim dd ON STR_TO_DATE(er.date, '%d/%m/%Y') = dd.date
            JOIN currency_dim cd ON er.currency_code = cd.currency_code
            ;
            """
        })
        
        for q in queries:
            print(f"Đang thực hiện: {q['description']}")
            if self.transform_table(self.conn_warehouse, q["query"]):
                print(f"{q['description']} thành công.")
            else:
                print(f"{q['description']} thất bại.")
        
    def close_connections(self):
        # Đóng tất cả kết nối database khi hoàn tất
        self.conn_control.close()
        self.conn_staging.close()
        self.conn_warehouse.close()

if __name__ == "__main__":
    try:
        # Nhận tham số từ dòng lệnh (ngày và ngân hàng)
        if len(sys.argv) > 1:
            input_date = sys.argv[1]
            try:
                date_obj = datetime.strptime(input_date, '%Y%m%d')
            except ValueError:
                error_message = "Định dạng ngày không hợp lệ. Vui lòng sử dụng định dạng YYYYMMDD."
                write_log("Date Format Error", error_message)
                print(error_message)
                sys.exit(1)
        else:
            # Mặc định là ngày hôm nay
            date_obj = datetime.now()

        # Tạo name cho tiến trình
        bank_name = "vcb"  # Thay đổi theo yêu cầu
        process_name = f"tygia_{bank_name}_{date_obj.strftime('%Y%m%d')}"

        # Thực hiện transform
        transform = Transform()
        # 3. Kiểm tra có tiến trình nào status= P và destination = W
        if not transform.check_processing("P", "W"):
            print(f"Không có tiến trình đang chạy. Đang kiểm tra tiến trình: {process_name}")
            try:
                # 4. Lấy thông tin của tiến trình transform chưa chạy (Status = C, Destination = S)
                process = transform.get_process_wh(process_name, "C", "S")
                # 5.Cập nhật trạng thái để chạy tiến trình (status= P)
                transform.update_status(process["id"], "P", "Process data transform")
                # 6. Cập nhật destination để chạy tiến trình (destination = W)
                transform.update_destination(process["id"])
                # 7. Tiến hành transform dữ liệu
                transform.transform_data()
                # 8. Cập nhật lại trạng thái (Statucs =C)
                transform.update_status(process["id"], "C", "Transform data successful")
            except Exception as e:
                # Ghi log nếu có lỗi xảy ra trong quá trình xử lý
                write_log("Process Error", str(e))
                print(f"Lỗi: {str(e)}")
        else:
            # 3.1. Thông báo Có tiến trình đang chạy
            print("Đã có tiến trình đang chạy.")
        # 9. Đóng kết nối    
        transform.close_connections()
    except Exception as e:
        # Ghi log nếu có lỗi xảy ra trong mã chính
        write_log("Main Execution Error", str(e))
        print(f"Lỗi: {str(e)}")
