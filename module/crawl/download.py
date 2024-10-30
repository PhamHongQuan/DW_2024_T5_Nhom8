from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

# Chỉ định đường dẫn thư mục muốn lưu file
download_dir = "D:\\DW_2024_T5_Nhom8\\file\\crawl\\vcb"

# Cấu hình tùy chọn Chrome để tải về một thư mục cụ thể
options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,  # Thư mục chỉ định
    "download.prompt_for_download": False,       # Tự động tải mà không hỏi
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True                 # Bật bảo mật
}
options.add_experimental_option("prefs", prefs)

# Khởi tạo driver với cấu hình tùy chọn
driver = webdriver.Chrome(options=options)

url = "https://www.vietcombank.com.vn/KHCN/Cong-cu-tien-ich/Ty-gia"  # Đặt URL của trang web ở đây

try:
    # Mở trang web
    driver.get(url)

    # Tìm button download (thay thế bằng phương thức tìm kiếm phù hợp với trang web của bạn)
    download_button = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, "//button[@id='btn-export-excel']"))
    )

    driver.execute_script("arguments[0].scrollIntoView();", download_button)

    # Chọn ngày trước khi thực hiện click
    date_input = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@id='datePicker']"))
    )
    # Gửi ngày bạn muốn chọn (thay thế 'your_date' bằng ngày bạn muốn)
    date_input.clear()  # Xóa giá trị ngày hiện tại nếu có
    date_input.send_keys("02-11-2023")
    date_input.send_keys(Keys.RETURN)  # Gửi phím RETURN để xác nhận

    driver.execute_script("arguments[0].click();", download_button)

    print("Đã nhận")
    # Click vào button download
    download_button.click()

    # Đợi một khoảng thời gian để đảm bảo file đã tải xong (thời gian này có thể cần điều chỉnh)
    WebDriverWait(driver, 30).until(
        lambda driver: len(driver.window_handles) > 1
    )

except Exception as e:
    print("Đã xảy ra lỗi:", e)

finally:
    # Đóng trình duyệt
    driver.quit()
