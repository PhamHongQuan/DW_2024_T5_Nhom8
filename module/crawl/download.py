from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


option = webdriver.ChromeOptions()
driver = webdriver.Chrome(options = option)

driver.get('https://www.google.com/')


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

    print("da nhan")
    # Click vào button download
    download_button.click()

    # Đợi một khoảng thời gian để đảm bảo file đã tải xong (thời gian này có thể cần điều chỉnh)
    WebDriverWait(driver, 30).until(
        lambda driver: len(driver.window_handles) > 1
    )

    # Chuyển đổi sang cửa sổ mới (assumed là cửa sổ download)
    driver.switch_to.window(driver.window_handles[1])

    # Lấy tên file đã tải (giả sử nó xuất hiện trong tiêu đề cửa sổ tải)
    downloaded_file_name = driver.title

    print("File downloaded:", downloaded_file_name)

except Exception as e:
    print("An error occurred:", e)

finally:
    # Đóng trình duyệt
    driver.quit()

# backup 1
# from selenium import webdriver
# from selenium.webdriver import Keys
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
#
#
# option = webdriver.ChromeOptions()
# driver = webdriver.Chrome(options = option)
#
# driver.get('https://www.google.com/')
#
#
# url = "https://www.vietcombank.com.vn/KHCN/Cong-cu-tien-ich/Ty-gia"  # Đặt URL của trang web ở đây
#
# try:
#     # Mở trang web
#     driver.get(url)
#
#     # Tìm button download (thay thế bằng phương thức tìm kiếm phù hợp với trang web của bạn)
#     download_button = WebDriverWait(driver, 60).until(
#         EC.element_to_be_clickable((By.XPATH, "//button[@id='btn-export-excel']"))
#     )
#
#     driver.execute_script("arguments[0].scrollIntoView();", download_button)
#
#     # Chọn ngày trước khi thực hiện click
#     # date_input = WebDriverWait(driver, 60).until(
#     #     EC.element_to_be_clickable((By.XPATH, "//input[@id='datePicker']"))
#     # )
#     # Gửi ngày bạn muốn chọn (thay thế 'your_date' bằng ngày bạn muốn)
#     # date_input.clear()  # Xóa giá trị ngày hiện tại nếu có
#     # date_input.send_keys("02-11-2023")
#     # date_input.send_keys(Keys.RETURN)  # Gửi phím RETURN để xác nhận
#
#     driver.execute_script("arguments[0].click();", download_button)
#
#     print("da nhan")
#     # Click vào button download
#     download_button.click()
#
#     # Đợi một khoảng thời gian để đảm bảo file đã tải xong (thời gian này có thể cần điều chỉnh)
#     WebDriverWait(driver, 30).until(
#         lambda driver: len(driver.window_handles) > 1
#     )
#
#     # Chuyển đổi sang cửa sổ mới (assumed là cửa sổ download)
#     driver.switch_to.window(driver.window_handles[1])
#
#     # Lấy tên file đã tải (giả sử nó xuất hiện trong tiêu đề cửa sổ tải)
#     downloaded_file_name = driver.title
#
#     print("File downloaded:", downloaded_file_name)
#
# except Exception as e:
#     print("An error occurred:", e)
#
# finally:
#     # Đóng trình duyệt
#     driver.quit()
