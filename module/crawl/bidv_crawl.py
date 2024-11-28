import io
import sys
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os

# Đảm bảo rằng đầu ra được mã hóa theo UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Nhận ngày từ đối số
if len(sys.argv) < 2:
    print("Ngày không được truyền vào.")
    sys.exit(1)

input_date = sys.argv[1]  # Ngày được truyền từ Java
# input_date = "01/11/2024"
# Đường dẫn của thư mục bạn muốn lưu file
output_folder = "D:\\DW_2024_T5_Nhom8\\file\\crawl\\bidv"
url = "https://bidv.com.vn/vn/ty-gia-ngoai-te"

# Cấu hình trình duyệt
option = webdriver.ChromeOptions()
driver = webdriver.Chrome(options=option)

data = []  # Danh sách chứa dữ liệu
bank_name = "BIDV"  # Tên ngân hàng

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

try:
    driver.get(url)

    # Đợi trang tải
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "table-reponsive")))

    # Nhập ngày vào trường lịch
    date_picker = driver.find_element(By.ID, "filter-by-start-date")  # Tìm trường lịch theo ID
    date_picker.clear()
    date_picker.send_keys(input_date)

    # Thêm thời gian chờ dài hơn để JavaScript có thể xử lý
    time.sleep(1)

    # Tìm và nhấn nút "TÌM KIẾM"
    search_button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "clickSearch"))
    )
    search_button.click()

    # Chờ thêm thời gian để đảm bảo dữ liệu tải xong
    time.sleep(1)

    # Đợi cho đến khi trang web tải hoàn tất dữ liệu sau khi tìm kiếm
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "table-reponsive")))

    # Lấy nội dung HTML đã tải
    html = driver.page_source

    # Tiếp tục xử lý HTML bằng BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Xác định bảng
    table = soup.find('table', class_='table-reponsive')

    if table:
        # Lấy các dòng từ tbody
        rows = table.select('tbody tr')

        for row in rows:
            row_data = []  # Danh sách chứa dữ liệu từ mỗi dòng

            # Lấy các ô từ mỗi dòng
            cells = row.select('td')

            for cell in cells:
                currency_code = cell.select_one('span.mobile-thead').text.strip()
                content_spans = cell.select('span.mobile-content span.ng-binding')

                if content_spans:
                    content = content_spans[-1].text.strip() if currency_code == 'Tên ngoại tệ' else content_spans[
                        0].text.strip()
                    row_data.append(content)
                else:
                    row_data.append("Không có dữ liệu")

            data.append(row_data)

finally:
    driver.quit()

# Tạo DataFrame từ danh sách dữ liệu
df = pd.DataFrame(data, columns=["Ký hiệu ngoại tệ", "Tên ngoại tệ", "Mua tiền mặt và Séc", "Mua chuyển khoản", "Bán"])
df["Bank Name"] = bank_name
df["Date"] = datetime.strptime(input_date, "%d/%m/%Y").strftime("%Y-%m-%d")

# Định dạng tên file theo ngày được chọn
formatted_date = datetime.strptime(input_date, "%d/%m/%Y").strftime("%Y%m%d")
excel_filename = f"{output_folder}/bidv_data_{formatted_date}.csv"

# Lưu DataFrame ra file CSV với mã hóa UTF-8
df.to_csv(excel_filename, index=False, encoding="utf-8-sig")
print(f"File CSV đã được lưu tại: {excel_filename}")
