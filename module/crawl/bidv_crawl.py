import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
import os

# Đường dẫn của thư mục bạn muốn lưu file
output_folder = "D:\\DW_2024_T5_Nhom8\\file\\crawl\\bidv"
url = "https://bidv.com.vn/vn/ty-gia-ngoai-te"

option = webdriver.ChromeOptions()
driver = webdriver.Chrome(options=option)

data = []  # Danh sách chứa dữ liệu
bank_name = "BIDV"  # Tên ngân hàng

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

try:
    driver.get(url)

    # Đợi cho đến khi trang web tải hoàn tất (có thể thay đổi timeout theo nhu cầu)
    timeout = 10
    start_time = time.time()
    while time.time() - start_time < timeout:
        if "document.readyState === 'complete'" in driver.execute_script("return document.readyState"):
            break
        time.sleep(1)

    # Lấy nội dung HTML đã tải
    html = driver.page_source

    # Tiếp tục xử lý HTML bằng BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Xác định bảng
    table = soup.find('table', class_='table-reponsive')

    # Kiểm tra xem bảng có tồn tại không
    if table:
        # Lấy các dòng từ tbody
        rows = table.select('tbody tr')

        for row in rows:
            row_data = []  # Danh sách chứa dữ liệu từ mỗi dòng

            # Lấy các ô từ mỗi dòng
            cells = row.select('td')

            # In thông tin từ mỗi ô
            for cell in cells:
                # Kiểm tra xem có phần tử mong muốn trong cell không
                currency_code = cell.select_one('span.mobile-thead').text.strip()
                content_spans = cell.select('span.mobile-content span.ng-binding')

                if content_spans:
                    # Lấy giá trị của thẻ cuối cùng nếu currency_code là 'Tên ngoại tệ'
                    content = content_spans[-1].text.strip() if currency_code == 'Tên ngoại tệ' else content_spans[
                        0].text.strip()
                    row_data.append(content)
                else:
                    row_data.append("Không có dữ liệu")

            # Thêm dữ liệu từ mỗi dòng vào danh sách chính
            data.append(row_data)

    else:
        print("Không tìm thấy bảng")

finally:
    # Đóng trình duyệt sau khi hoàn thành
    driver.quit()

# Tạo DataFrame từ danh sách dữ liệu
df = pd.DataFrame(data, columns=["Ký hiệu ngoại tệ", "Tên ngoại tệ", "Mua tiền mặt và Séc", "Mua chuyển khoản", "Bán"])
# Thêm hai cột mới
df["Bank Name"] = bank_name
df["Date"] = datetime.now().strftime("%Y-%m-%d")

# Lấy ngày và thời gian hiện tại
current_datetime = datetime.now().strftime("%Y%m%d")
current_datetime = datetime.now().strftime("%Y%m%d_%H%M")

# Tạo tên file với định dạng "vietcombank_data_<ngày>_<giờ>.xlsx"
excel_filename = f"{output_folder}/bidv_data_{current_datetime}.csv"
df.to_csv(excel_filename, index=False)
