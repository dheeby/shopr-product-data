package shopr.productdata.dataloaders;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import shopr.productdata.utils.Constants;
import shopr.productdata.utils.PropertiesLoader;

import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by Neil on 9/4/2016.
 * @author Neil Allison
 */
public class BestBuyDataPipeline
{
    private static final Logger logger = Logger.getLogger(BestBuyDataPipeline.class);

    private static long totalBytesRead = 0;

    public static String downloadProductData(String destinationDir)
    {
        logger.info("Starting BestBuy bulk data download");
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(getProductsApiUrlString());
        HttpResponse httpResponse;
        try
        {
            httpResponse = httpClient.execute(request);
        }
        catch (IOException e)
        {
            logger.error("Exception executing BestBuy bulk data load request", e);
            return null;
        }

        logger.info(httpResponse.getStatusLine());

        String bulkDataFilename = Paths.get(destinationDir, createCompressedProductDataFilename()).toString();

        try (
                InputStream is = httpResponse.getEntity().getContent();
                FileOutputStream fos = new FileOutputStream(bulkDataFilename)
        )
        {
            long contentLength = Long.parseLong(httpResponse.getFirstHeader("Content-length").getValue());

            logger.info("BestBuy bulk data file size: " + (contentLength / 1048576) + "MiB");
            logger.info("Writing BestBuy bulk data response to file...");
            byte[] buffer = new byte[8192];
            int bytesRead;
            DownloadProgress downloadProgress = new DownloadProgress(contentLength);
            downloadProgress.start();

            while ((bytesRead = is.read(buffer)) > 0)
            {
                fos.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }
        }
        catch (FileNotFoundException e)
        {
            logger.error("Exception attempting to create output stream for BestBuy bulk data file");
            return null;
        }
        catch (IOException e)
        {
            logger.error("Exception retrieving/writing BestBuy bulk data load response content", e);
            return null;
        }
        logger.info("Finished BestBuy bulk data download");
        return bulkDataFilename;
    }

    public static String unzipBulkDataFile(String compressedFile, String destinationDir)
    {
        long startTime = System.currentTimeMillis();

        logger.info("Unzipping BestBuy bulk data file: " + compressedFile);

        File outputDir = new File(destinationDir);
        if (!outputDir.exists())
        {
            logger.info("Creating output directory: " + destinationDir);
            outputDir.mkdir();
        }

        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(compressedFile)))
        {
            ZipEntry zipEntry = zis.getNextEntry();
            byte[] buffer = new byte[8192];

            while (zipEntry != null)
            {
                String filename = zipEntry.getName();
                File currentFile = new File(outputDir + File.separator + filename);
                new File(currentFile.getParent()).mkdirs();
                logger.info("Decompressing file: " + filename);

                FileOutputStream fos = new FileOutputStream(currentFile);
                int bytesRead;
                while ((bytesRead = zis.read(buffer)) > 0)
                {
                    fos.write(buffer, 0, bytesRead);
                }
                fos.close();

                logger.info("Decompressing complete");
                zipEntry = zis.getNextEntry();
            }
        }
        catch (IOException e)
        {
            logger.error("Exception unzipping BestBuy bulk data file", e);
            return null;
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        long minutes = elapsedTime / 60000;
        long seconds = (elapsedTime - (60000 * minutes)) / 1000;
        long milliseconds = elapsedTime - (60000 * minutes) - (1000 * seconds);
        logger.info(String.format("Unzipping complete. Elapsed Time: %dmin, %ds, %dms", minutes, seconds, milliseconds));
        return destinationDir;
    }

    public String cleanData(String dataDirectory)
    {
        File dataDir = new File(dataDirectory);


        return dataDirectory;
    }

    private static String getProductsApiUrlString()
    {
        return Constants.BESTBUY_BULK_PRODUCT_API_BASE + PropertiesLoader.getInstance().getProperty("bestbuy.apikey");
    }


    private static String createCompressedProductDataFilename()
    {
        return String.format("%s_products_BestBuy.json.zip", createDatePrefix());
    }

    private static String createDatePrefix()
    {
        DateTime dt = new DateTime();
        return String.format("%04d-%02d-%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
    }

    private static class DownloadProgress extends Thread
    {
        private long totalBytesToDownload;

        public DownloadProgress(long totalBytesToDownload)
        {
            this.totalBytesToDownload = totalBytesToDownload;
        }

        public void run()
        {
            try
            {
                while (totalBytesRead < totalBytesToDownload)
                {
                    logger.info(String.format("Progress: %.2f%%", 100.0 * totalBytesRead / totalBytesToDownload));
                    TimeUnit.SECONDS.sleep(5);
                }
            }
            catch (InterruptedException e)
            {
                logger.warn("Exception sleeping in DownloadProgress", e);
            }
        }
    }
}
