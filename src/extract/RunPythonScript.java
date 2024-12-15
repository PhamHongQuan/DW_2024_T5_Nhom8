package extract;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunPythonScript {
    public static boolean runScriptPy(String scriptPath, String inputDate) {
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

    public static void main(String[] args) {
        // Đường dẫn đến script Python (có thể thay đổi)
        String pythonScriptPath1 = "D:\\DW_2024_T5_Nhom8\\module\\crawl\\bidv_crawl.py";
        String pythonScriptPath2 = "D:\\DW_2024_T5_Nhom8\\module\\crawl\\vcb_crawl.py";
        String inputDate = "01/11/2024";  // Ngày mà bạn muốn crawl, có thể thay đổi

//        boolean scriptResult1 = runScriptPy(pythonScriptPath1, inputDate);
        boolean scriptResult2 = runScriptPy(pythonScriptPath2, inputDate);

//        if (scriptResult1) {
//            System.out.println("Script executed successfully.");
//        } else {
//            System.out.println("Script failed.");
//        }

        if (scriptResult2) {
            System.out.println("Script executed successfully.");
        } else {
            System.out.println("Script failed.");
        }
    }

    public boolean runScript(String scriptPath) {
        try {
            // Execute the script using the ProcessBuilder class or Runtime.exec
            ProcessBuilder processBuilder = new ProcessBuilder("python", scriptPath);
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            return exitCode == 0;  // If the script ran successfully, the exit code will be 0.
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }}
