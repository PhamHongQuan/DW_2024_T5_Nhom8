import io
import os
import time
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
from selenium import webdriver
import pyperclip  # Thư viện hỗ trợ copy/paste

# Đảm bảo rằng đầu ra được mã hóa theo UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Nhận ngày từ đối số
if len(sys.argv) < 2:
    print("Ngày không được truyền vào.")
    sys.exit(1)

input_date = sys.argv[1]  # Ngày được truyền từ Java

# input_date = "01/11/2024"  # Ngày cần chọn

# Thiết lập ChromeDriver
chrome_options = Options()
chrome_options.headless = False  # Để dễ kiểm tra (True nếu không cần hiển thị trình duyệt)

# Khởi tạo trình duyệt
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Truy cập vào trang web Vietcombank
url = "https://vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
driver.get(url)

# Lấy ô chọn ngày
date_picker = driver.find_element(By.ID, "datePicker")

# Copy giá trị ngày vào clipboard
pyperclip.copy(input_date)


action = ActionChains(driver)
action.move_to_element(date_picker).click().perform()



action = ActionChains(driver)
action.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()  # Chọn tất cả nội dung
action.key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL).perform()  # Dán nội dung từ clipboard

# Đợi vài giây để trang web cập nhật
time.sleep(5)

# # Tìm và nhấn vào nút "Xem thêm"
# try:
#     load_more_button = driver.find_element(By.ID, "load-more-label")  # Dùng ID của nút "Xem thêm"
#     load_more_button.click()  # Nhấn nút "Xem thêm"
#     time.sleep(3)  # Đợi vài giây để tải thêm dữ liệu
# except Exception as e:
#     print("Không tìm thấy nút 'Xem thêm'. Lỗi:", e)

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