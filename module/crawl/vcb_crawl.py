import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium import webdriver

# Thiết lập ChromeDriver
chrome_options = Options()
chrome_options.headless = True  # Chạy ở chế độ headless (không hiển thị trình duyệt)

# Khởi tạo trình duyệt
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Truy cập vào trang web Vietcombank
url = "https://vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
driver.get(url)

# Nhập ngày vào trường chọn ngày bằng JavaScript
input_date = "01/11/2024"  # Ngày bạn muốn chọn
date_picker = driver.find_element(By.ID, "datePicker")

# Sử dụng JavaScript để thay đổi giá trị của trường chọn ngày
driver.execute_script(f"arguments[0].value = '{input_date}'", date_picker)

# Đợi vài giây để trang web cập nhật sau khi nhập ngày
time.sleep(3)

# Kiểm tra lại giá trị đã nhập vào trường ngày (tránh trường hợp nhập sai định dạng)
current_value = date_picker.get_attribute("value")
print(f"Ngày đã nhập vào trường chọn ngày: {current_value}")

# Trích xuất dữ liệu tỷ giá từ trang web
exchange_rates = driver.find_elements(By.CSS_SELECTOR, "table.table-responsive tbody tr")

data = []
for rate in exchange_rates:
    cols = rate.find_elements(By.TAG_NAME, "td")
    if len(cols) == 5:  # Đảm bảo có đủ 5 cột
        currency_code = cols[0].text.strip()  # Mã ngoại tệ
        currency_name = cols[1].text.strip()  # Tên ngoại tệ
        buy = cols[2].text.strip()  # Mua tiền mặt
        transfer = cols[3].text.strip()  # Mua chuyển khoản
        sell = cols[4].text.strip()  # Bán

        # Thêm thông tin về ngân hàng và ngày vào dữ liệu
        data.append({
            "Currency Code": currency_code,
            "Currency Name": currency_name,
            "Buy": buy,
            "Transfer": transfer,
            "Sell": sell,
            "BankName": "VCB",  # Thêm cột "BankName"
            "Date": input_date  # Sử dụng ngày bạn nhập vào
        })

# Chuyển dữ liệu thành DataFrame
df = pd.DataFrame(data)

# Chuyển định dạng ngày sang yyyyMMdd
formatted_date = input_date.replace('/', '')  # Đây là cách loại bỏ dấu '/' nếu bạn muốn
formatted_date = f"{input_date[6:]}{input_date[3:5]}{input_date[:2]}"  # Đổi lại thành yyyyMMdd

# Lưu dữ liệu vào file CSV
folder_selected = "D:\\DW_2024_T5_Nhom8\\file\\crawl\\vcb"
if not os.path.exists(folder_selected):
    os.makedirs(folder_selected)

excel_filename = f"{folder_selected}/vietcombank_data_{formatted_date}.csv"

# Lưu DataFrame ra file CSV với mã hóa UTF-8
df.to_csv(excel_filename, index=False, encoding="utf-8-sig")
print("Data saved to", excel_filename)


# Đóng trình duyệt
driver.quit()
