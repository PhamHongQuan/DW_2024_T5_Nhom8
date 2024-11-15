package extract;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPythonScript {
    public static boolean runScript(String scriptPath, String inputDate) {
        try {
            // Truyền ngày vào script Python qua đối số
            ProcessBuilder processBuilder = new ProcessBuilder("python", scriptPath, inputDate);
            Process process = processBuilder.start();

            // Đọc output của lệnh Python (nếu có)
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Đọc error của lệnh Python (nếu có)
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((line = errorReader.readLine()) != null) {
                System.err.println(line);
            }

            int exitCode = process.waitFor();

            // Return true if the exit code is 0 (success), otherwise return false
            return exitCode == 0;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            // Return false in case of an exception
            return false;
        }
    }

//    public static void main(String[] args) {
//        // Đường dẫn đến script Python (có thể thay đổi)
//        String pythonScriptPath = "D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py";
//        String inputDate = "01/11/2024";  // Ngày mà bạn muốn crawl, có thể thay đổi
//
//        boolean scriptResult = runScript(pythonScriptPath, targetDate);
//
//        if (scriptResult) {
//            System.out.println("Script executed successfully.");
//        } else {
//            System.out.println("Script failed.");
//        }
//    }
}
