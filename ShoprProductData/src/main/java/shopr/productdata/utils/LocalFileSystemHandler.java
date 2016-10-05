package shopr.productdata.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by Neil on 9/27/2016.
 *
 * @author Neil Allison
 */
public class LocalFileSystemHandler
{
    private static final Logger LOGGER = Logger.getLogger(LocalFileSystemHandler.class);

    public static boolean createDirectory(String dstDir)
    {
        try
        {
            Files.createDirectories(Paths.get(dstDir));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to create temporary directory: " + dstDir, e);
            return false;
        }
        return true;
    }

    public static boolean deleteDirectory(String targetDir)
    {
        try
        {
            FileUtils.forceDelete(new File(targetDir));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to delete temporary directory: " + targetDir, e);
            return false;
        }
        return true;
    }

    public static boolean zipFiles(String zipOutputFilePath, String inputFilesDir) throws IOException
    {
        FileOutputStream fos = new FileOutputStream(zipOutputFilePath);
        ZipOutputStream zos = new ZipOutputStream(fos);

        File[] productDataFiles = (new File(inputFilesDir)).listFiles();
        if (productDataFiles == null || productDataFiles.length == 0)
        {
            return false;
        }
        for (File dataFile : productDataFiles)
        {
            ZipEntry zipEntry = new ZipEntry(dataFile.getName());
            zos.putNextEntry(zipEntry);

            FileInputStream fis = new FileInputStream(dataFile.getAbsolutePath());

            int bytesRead;
            byte[] buffer = new byte[8192];
            while ((bytesRead = fis.read(buffer)) > 0)
            {
                zos.write(buffer, 0, bytesRead);
            }

            fis.close();
        }

        zos.flush();
        zos.close();
        fos.flush();
        fos.close();
        return true;
    }
}
