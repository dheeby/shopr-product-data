package shopr.productdata.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Neil on 9/27/2016.
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
}
