package shopr.productdata.dataloaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import shopr.productdata.objects.BestBuyProduct;
import shopr.productdata.utils.Constants;
import shopr.productdata.utils.PropertiesLoader;

import java.io.*;
import java.nio.file.Files;
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

        File outputDir = new File(destinationDir + File.separator + "unzipped");
        if (!outputDir.exists())
        {
            logger.info("Creating unzipped output directory: " + destinationDir);
            if (!outputDir.mkdir())
            {
                logger.error("Failed to create unzipped output directory");
                return null;
            }
        }

        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(compressedFile)))
        {
            ZipEntry zipEntry = zis.getNextEntry();
            byte[] buffer = new byte[8192];

            while (zipEntry != null)
            {
                String filename = zipEntry.getName();
                File currentFile = new File(createUnzippedDataFilePath(destinationDir, filename));
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
        logger.info(String.format("Unzipping complete. Elapsed Time: %s", formatTime(elapsedTime)));

        try
        {
            Files.delete(Paths.get(compressedFile));
            logger.info("Compressed bulk data file deleted");
        }
        catch (IOException e)
        {
            logger.warn("Failed to delete compressed data file");
        }
        return outputDir.getAbsolutePath();
    }

    @SuppressWarnings("unchecked")
    public static String cleanData(String dataDirectory)
    {
        logger.info("Starting data clean phase for data directory: " + dataDirectory);

        ObjectMapper mapper = new ObjectMapper();
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        JSONParser jsonParser = new JSONParser();

        File dataDir = new File(dataDirectory);
        File[] dataFiles = dataDir.listFiles();
        if (dataFiles == null)
        {
            logger.info("Failed to get data files from data directory: " + dataDirectory);
            return null;
        }

        File outputDir = new File(PropertiesLoader.getInstance().getProperty("dir.tmp.destination") + File.separator + "cleaned");
        if (!outputDir.exists())
        {
            logger.info("Creating cleaned output directory: " + outputDir.getAbsolutePath());
            if (!outputDir.mkdir())
            {
                logger.error("Failed to create cleaned output directory");
                return null;
            }
        }

        for (File dataFile : dataFiles)
        {
            ArrayNode productDataArrayNode = nodeFactory.arrayNode();
            String dataFilePath = dataFile.getAbsolutePath();
            logger.info("Parsing data file: " + dataFilePath);
            try
            {
                JSONArray productDataArray = (JSONArray) jsonParser.parse(new FileReader(dataFile));
                for (Object productObject : productDataArray)
                {
                    JSONObject product = (JSONObject) productObject;

                    BestBuyProduct bestBuyProduct = new BestBuyProduct();
                    bestBuyProduct.setSku((Long) product.get("sku"));
                    bestBuyProduct.setProductId((Long) product.get("productId"));
                    bestBuyProduct.setName((String) product.get("name"));
                    bestBuyProduct.setType((String) product.get("type"));
                    bestBuyProduct.setRegularPrice((Double) product.get("regularPrice"));
                    bestBuyProduct.setSalePrice((Double) product.get("salePrice"));
                    bestBuyProduct.setOnSale((Boolean) product.get("onSale"));
                    bestBuyProduct.setImage((String) product.get("image"));
                    bestBuyProduct.setThumbnailImage((String) product.get("thumbnailImage"));
                    bestBuyProduct.setShortDescription((String) product.get("shortDescription"));
                    bestBuyProduct.setLongDescription((String) product.get("longDescription"));
                    bestBuyProduct.setCustomerReviewCount((Long) product.get("customerReviewCount"));
                    bestBuyProduct.setCustomerReviewAverage((String) product.get("customerReviewAverage"));

                    productDataArrayNode.addPOJO(bestBuyProduct);
                }
            }
            catch (ParseException e)
            {
                logger.error("Parsing data file failed: " + dataFilePath, e);
            }
            catch (IOException e)
            {
                logger.error("Opening data file failed", e);
            }

            String outFilePath = createCleanedDataFilePath(dataFile.getName());
            try (FileWriter outFile = new FileWriter(outFilePath))
            {
                logger.info("Writing cleaned data to file");
                outFile.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(productDataArrayNode));
                outFile.flush();
                logger.info("Writing cleaned data file complete: " + outFilePath);
            }
            catch (IOException e)
            {
                logger.error("Opening cleaned data file failed", e);
            }
        }

        return dataDirectory;
    }

    private static String formatTime(long timeInMillis)
    {
        long minutes = timeInMillis / 60000;
        long seconds = (timeInMillis - (60000 * minutes)) / 1000;
        long milliseconds = timeInMillis - (60000 * minutes) - (1000 * seconds);
        return String.format("%dmin, %ds, %dms", minutes, seconds, milliseconds);
    }

    private static String getProductsApiUrlString()
    {
        return Constants.BESTBUY_BULK_PRODUCT_API_BASE + PropertiesLoader.getInstance().getProperty("bestbuy.apikey");
    }

    private static String createUnzippedDataFilePath(String outputBaseDir, String filename)
    {
        return String.format("%s%s%s%s%s", outputBaseDir, File.separator, "unzipped", File.separator, filename);
    }

    private static String createCleanedDataFilePath(String filename)
    {
        return String.format("%s%s%s%s%s%s", PropertiesLoader.getInstance().getProperty("dir.tmp.destination"),
                File.separator, "cleaned", File.separator, "cleaned_", filename);
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
