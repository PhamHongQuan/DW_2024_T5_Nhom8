package extract;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CrawlVietcombank {
    public static void crawlVietcombank(String url, String folderSelected){
        try {
            // Kiểm tra nếu thư mục không tồn tại, tạo nó
            Path folderPath = Paths.get(folderSelected);
            if (!Files.exists(folderPath)) {
                Files.createDirectories(folderPath);
                System.out.println("Thư mục " + folderSelected + " đã được tạo.");
            }

            try {
                URL apiUrl = new URL(url);
                HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

                if (connection.getResponseCode() == 200) {
                    // Parse XML data
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    Document doc = dBuilder.parse(connection.getInputStream());
                    doc.getDocumentElement().normalize();

                    // Lấy giá trị từ các phần tử XML
                    String dateTime = doc.getElementsByTagName("DateTime").item(0).getTextContent();
                    String source = doc.getElementsByTagName("Source").item(0).getTextContent();

                    // Tạo danh sách dữ liệu từ các phần tử XML
                    List<String[]> data = new ArrayList<>();
                    NodeList exrateList = doc.getElementsByTagName("Exrate");
                    for (int i = 0; i < exrateList.getLength(); i++) {
                        Element exrateElem = (Element) exrateList.item(i);
                        String currencyCode = exrateElem.getAttribute("CurrencyCode");
                        String currencyName = exrateElem.getAttribute("CurrencyName");
                        String buy = exrateElem.getAttribute("Buy");
                        String transfer = exrateElem.getAttribute("Transfer");
                        String sell = exrateElem.getAttribute("Sell");

                        data.add(new String[]{currencyCode, currencyName, buy, transfer, sell});
                    }

                    // Tạo DataFrame từ dữ liệu XML
                    // (Trong Java, bạn có thể sử dụng thư viện Apache POI để tạo và ghi Excel)
                    // Tạo ngày và giờ hiện tại
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                    String currentDateTime = dateFormat.format(new Date());

                    // Tạo tên file với định dạng "vietcombank_data_<ngày>_<giờ>.csv"
                    String csvFileName = folderSelected + "\\vietcombank_data_" + currentDateTime + ".csv";

                    // Ghi dữ liệu vào file CSV
                    try (FileOutputStream outputStream = new FileOutputStream(new File(csvFileName))) {
                        for (String[] rowData : data) {
                            String row = String.join(",", rowData) + "\n";
                            outputStream.write(row.getBytes());
                        }
                        System.out.println("Data saved to " + csvFileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    System.out.println("Failed to retrieve data. Status code: " + connection.getResponseCode());
                }

            } catch (ParserConfigurationException | IOException | SAXException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }

        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
    }
    public static boolean crawlDataVietcombank(){
        String url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx";
        String folderSelected = "D:\\DW_2024_T5_Nhom8\\file\\crawl";
        try {
            // Kiểm tra nếu thư mục không tồn tại, tạo nó
            Path folderPath = Paths.get(folderSelected);
            if (!Files.exists(folderPath)) {
                Files.createDirectories(folderPath);
                System.out.println("Thư mục " + folderSelected + " đã được tạo.");
            }

            try {
                URL apiUrl = new URL(url);
                HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

                if (connection.getResponseCode() == 200) {
                    // Parse XML data
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    Document doc = dBuilder.parse(connection.getInputStream());
                    doc.getDocumentElement().normalize();

                    // Lấy giá trị từ các phần tử XML
                    String dateTime = doc.getElementsByTagName("DateTime").item(0).getTextContent();
                    String source = doc.getElementsByTagName("Source").item(0).getTextContent();

                    // Tạo danh sách dữ liệu từ các phần tử XML
                    List<String[]> data = new ArrayList<>();
                    NodeList exrateList = doc.getElementsByTagName("Exrate");
                    for (int i = 0; i < exrateList.getLength(); i++) {
                        Element exrateElem = (Element) exrateList.item(i);
                        String currencyCode = exrateElem.getAttribute("CurrencyCode");
                        String currencyName = exrateElem.getAttribute("CurrencyName");
                        String buy = exrateElem.getAttribute("Buy");
                        String transfer = exrateElem.getAttribute("Transfer");
                        String sell = exrateElem.getAttribute("Sell");
                        data.add(new String[]{currencyCode, currencyName, buy, transfer, sell});
                    }

                    // Tạo DataFrame từ dữ liệu XML
                    // (Trong Java, bạn có thể sử dụng thư viện Apache POI để tạo và ghi Excel)
                    // Tạo ngày và giờ hiện tại
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                    String currentDateTime = dateFormat.format(new Date());

                    // Tạo tên file với định dạng "vietcombank_data_<ngày>_<giờ>.csv"
                    String csvFileName = folderSelected + "\\vietcombank_data_" + currentDateTime + ".csv";

                    // Ghi dữ liệu vào file CSV
                    try (FileOutputStream outputStream = new FileOutputStream(new File(csvFileName))) {
                        for (String[] rowData : data) {
                            String row = String.join(",", rowData) + "\n";
                            outputStream.write(row.getBytes());
                        }
                        System.out.println("Data saved to " + csvFileName);
                        return true;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    System.out.println("Failed to retrieve data. Status code: " + connection.getResponseCode());
                }

            } catch (ParserConfigurationException | IOException | SAXException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }

        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
        return  false;
    }
    public static File crawlDataVietcombankFile() {
        String url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx";
       String folderSelected = "D:\\DW_2024_T5_Nhom8\\file\\crawl";
        // String folderSelected = "E:\\test\\DW_2023_T4_Nhom7-main\\file";
        try {
            // Kiểm tra nếu thư mục không tồn tại, tạo nó
            Path folderPath = Paths.get(folderSelected);
            if (!Files.exists(folderPath)) {
                try {
                    Files.createDirectories(folderPath);
                    System.out.println("Thư mục " + folderSelected + " đã được tạo.");
                } catch (FileAlreadyExistsException e) {
                    // Thư mục đã tồn tại, không cần tạo mới
                }
            }

            File csvFile = null;

            try {
                URL apiUrl = new URL(url);
                HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

                if (connection.getResponseCode() == 200) {
                    // Parse XML data
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    Document doc = dBuilder.parse(connection.getInputStream());
                    doc.getDocumentElement().normalize();

                    // Lấy giá trị từ các phần tử XML
                    String dateTime = doc.getElementsByTagName("DateTime").item(0).getTextContent();
                    String source = doc.getElementsByTagName("Source").item(0).getTextContent();

                    // Tạo danh sách dữ liệu từ các phần tử XML
                    List<String[]> data = new ArrayList<>();
                    NodeList exrateList = doc.getElementsByTagName("Exrate");
                    for (int i = 0; i < exrateList.getLength(); i++) {
                        Element exrateElem = (Element) exrateList.item(i);
                        String currencyCode = exrateElem.getAttribute("CurrencyCode");
                        String currencyName = exrateElem.getAttribute("CurrencyName");
                        String buy = exrateElem.getAttribute("Buy");
                        String transfer = exrateElem.getAttribute("Transfer");
                        String sell = exrateElem.getAttribute("Sell");

                        data.add(new String[]{currencyCode, currencyName, buy, transfer, sell});
                    }

                    // Tạo DataFrame từ dữ liệu XML
                    // (Trong Java, bạn có thể sử dụng thư viện Apache POI để tạo và ghi Excel)
                    // Tạo ngày và giờ hiện tại
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
                    String currentDateTime = dateFormat.format(new Date());

                    // Tạo tên file với định dạng "vietcombank_data_<ngày>_<giờ>.csv"
                    String csvFileName = folderSelected + "\\vietcombank_data_" + currentDateTime + ".csv";
                    csvFile = new File(csvFileName);

                    // Ghi dữ liệu vào file CSV
                    try (FileOutputStream outputStream = new FileOutputStream(csvFile)) {
                        for (String[] rowData : data) {
                            String row = String.join(",", rowData) + "\n";
                            outputStream.write(row.getBytes());
                        }
                        System.out.println("Data saved to " + csvFileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    System.out.println("Failed to retrieve data. Status code: " + connection.getResponseCode());
                }

            } catch (ParserConfigurationException | IOException | SAXException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }

            return csvFile;

        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
            return null;
        }
    }
    public static void main(String[] args) {
        String url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx";
        String folderSelected = "D:\\DW_2024_T5_Nhom8\\file\\crawl";

//       crawlVietcombank(url,folderSelected);
//        crawlDataVietcombank();
        File csvFile = crawlDataVietcombankFile();
        if (csvFile != null && csvFile.exists()) {
            // Thực hiện các thao tác kiểm tra hoặc xử lý với file CSV tại đây
            System.out.println("File exists: " + csvFile.getAbsolutePath());

        }
    }
}
