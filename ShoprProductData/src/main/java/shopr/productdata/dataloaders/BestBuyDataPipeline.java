package shopr.productdata.dataloaders;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.io.FileUtils;
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
import shopr.productdata.utils.EmailHandler;
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
    private static final Logger LOGGER = Logger.getLogger(BestBuyDataPipeline.class);
    private static final String PIPELINE_NAME = "BESTBUY";
    private static final String SHOPR_S3_DATA_BUCKET = "shopr-data.com";

    private static long totalBytesRead;

    public static boolean executeBestBuyDataPipeline()
    {
        LOGGER.info("Executing BestBuy data pipeline...");
        String dstDir = PropertiesLoader.getInstance().getProperty("dir.tmp.destination");
        if (Files.notExists(Paths.get(dstDir)))
        {
            createTemporaryDirectory(dstDir);
        }

        // Phase 1
        String compressedFile = downloadProductData(dstDir);
        if (compressedFile == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 1);
            // TODO: insert state into state table
            return false;
        }

        // Phase 2
        String unzippedDir = unzipBulkDataFile(compressedFile, dstDir);
        if (unzippedDir == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 2);
            // TODO: insert state into state table
            return false;
        }

        // Phase 3
        String cleanedDir = cleanData(unzippedDir);
        if (cleanedDir == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 3);
            // TODO: insert state into state table
            return false;
        }

        // Phase 4
        boolean uploadedToS3 = uploadCleanedProductDataToS3(cleanedDir);
        if (!uploadedToS3)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 4);
            // TODO: insert state into state table
            return false;
        }

        // Phase 5
        boolean insertedInDB = insertProductDataToDB(cleanedDir);
        if (!insertedInDB)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 5);
            // TODO: insert state into state table
            return false;
        }

        deleteTemporaryDirectory(dstDir);

        EmailHandler.sendSuccessEmail(PIPELINE_NAME);
        LOGGER.info("BestBuy data pipeline complete");
        return true;
    }

    public static String downloadProductData(String destinationDir)
    {
        LOGGER.info("Phase 1: Starting BestBuy bulk data download");
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(getProductsApiUrlString());
        HttpResponse httpResponse;
        try
        {
            httpResponse = httpClient.execute(request);
        }
        catch (IOException e)
        {
            LOGGER.error("Exception executing BestBuy bulk data load request", e);
            return null;
        }

        LOGGER.info(httpResponse.getStatusLine());

        String bulkDataFilename = createCompressedProductDataFilename();
        String bulkDataFilePath = Paths.get(destinationDir, bulkDataFilename).toString();

        try (
                InputStream is = httpResponse.getEntity().getContent();
                FileOutputStream fos = new FileOutputStream(bulkDataFilePath)
        )
        {
            long contentLength = Long.parseLong(httpResponse.getFirstHeader("Content-length").getValue());

            LOGGER.info("BestBuy bulk data file size: " + (contentLength / 1048576) + "MiB");
            LOGGER.info("Writing BestBuy bulk data response to file...");
            byte[] buffer = new byte[8192];
            int bytesRead;
            DownloadProgress downloadProgress = new DownloadProgress(contentLength);
            downloadProgress.start();

            while ((bytesRead = is.read(buffer)) > 0)
            {
                fos.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }

            fos.flush();
            fos.close();
        }
        catch (FileNotFoundException e)
        {
            LOGGER.error("Exception attempting to create output stream for BestBuy bulk data file");
            return null;
        }
        catch (IOException e)
        {
            LOGGER.error("Exception retrieving/writing BestBuy bulk data load response content", e);
            return null;
        }
        LOGGER.info("Finished BestBuy bulk data download");
        return bulkDataFilePath;
    }

    public static String unzipBulkDataFile(String compressedFile, String destinationDir)
    {
        LOGGER.info("Phase 2: Unzipping BestBuy bulk data file: " + compressedFile);
        long startTime = System.currentTimeMillis();

        File outputDir = new File(destinationDir + File.separator + "unzipped");
        if (!outputDir.exists())
        {
            LOGGER.info("Creating unzipped output directory: " + destinationDir);
            if (!outputDir.mkdir())
            {
                LOGGER.error("Failed to create unzipped output directory");
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
                LOGGER.info("Decompressing file: " + filename);

                FileOutputStream fos = new FileOutputStream(currentFile);
                int bytesRead;
                while ((bytesRead = zis.read(buffer)) > 0)
                {
                    fos.write(buffer, 0, bytesRead);
                }
                fos.flush();
                fos.close();

                LOGGER.info("Decompressing complete");
                zipEntry = zis.getNextEntry();
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Exception unzipping BestBuy bulk data file", e);
            return null;
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOGGER.info(String.format("Unzipping complete. Elapsed Time: %s", formatTime(elapsedTime)));

        try
        {
            Files.delete(Paths.get(compressedFile));
            LOGGER.info("Compressed bulk data file deleted");
        }
        catch (IOException e)
        {
            LOGGER.warn("Failed to delete compressed data file");
        }
        return outputDir.getAbsolutePath();
    }

    @SuppressWarnings("unchecked")
    public static String cleanData(String dataDirectory)
    {
        LOGGER.info("Phase 3: Starting data clean phase for data directory: " + dataDirectory);

        ObjectMapper mapper = new ObjectMapper();
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        JSONParser jsonParser = new JSONParser();

        File dataDir = new File(dataDirectory);
        File[] dataFiles = dataDir.listFiles();
        if (dataFiles == null)
        {
            LOGGER.error("Failed to get data files from data directory: " + dataDirectory);
            return null;
        }

        File outputDir = new File(PropertiesLoader.getInstance().getProperty("dir.tmp.destination") + File.separator + "cleaned");
        if (!outputDir.exists())
        {
            LOGGER.info("Creating cleaned output directory: " + outputDir.getAbsolutePath());
            if (!outputDir.mkdir())
            {
                LOGGER.error("Failed to create cleaned output directory");
                return null;
            }
        }

        for (File dataFile : dataFiles)
        {
            ArrayNode productDataArrayNode = nodeFactory.arrayNode();
            String dataFilePath = dataFile.getAbsolutePath();
            LOGGER.info("Parsing data file: " + dataFilePath);
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
                LOGGER.error("Parsing data file failed: " + dataFilePath, e);
                return null;
            }
            catch (IOException e)
            {
                LOGGER.error("Opening data file failed", e);
                return null;
            }

            String outFilePath = createCleanedDataFilePath(dataFile.getName());
            try (FileWriter outFile = new FileWriter(outFilePath))
            {
                LOGGER.info("Writing cleaned data to file");
                outFile.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(productDataArrayNode));
                outFile.flush();
                outFile.close();
                LOGGER.info("Writing cleaned data file complete: " + outFilePath);
            }
            catch (IOException e)
            {
                LOGGER.error("Opening cleaned data file failed", e);
                return null;
            }
        }

        return outputDir.getAbsolutePath();
    }

    private static boolean uploadCleanedProductDataToS3(String productDataDirectory)
    {
        LOGGER.info("Phase 4: Uploading product data to S3");

        File[] productDataFiles = (new File(productDataDirectory)).listFiles();
        if (productDataFiles == null || productDataFiles.length == 0)
        {
            LOGGER.warn("No files available to upload");
            return false;
        }
        for (File productDataFile : productDataFiles)
        {
            String dataFilename = productDataFile.getName();
            LOGGER.info("Uploading file to S3: " + dataFilename);
            if (!uploadToS3(SHOPR_S3_DATA_BUCKET, createS3ParsedDataDirName(), productDataFile))
            {
                LOGGER.warn("Upload failure");
            }
            else
            {
                LOGGER.info("Upload success");
            }
        }

        return true;
    }

    private static boolean insertProductDataToDB(String productDataDirectory)
    {
        LOGGER.info("Phase 5: Inserting product data into database");
        // TODO: insert product data into DB
        LOGGER.info("Loading from data directory: " + productDataDirectory);
        return true;
    }

    private static boolean createTemporaryDirectory(String dstDir)
    {
        try
        {
            Files.createDirectory(Paths.get(dstDir));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to create temporary directory");
            return false;
        }
        return true;
    }

    private static boolean deleteTemporaryDirectory(String dstDir)
    {
        try
        {
            FileUtils.forceDelete(new File(dstDir));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to delete temporary directory", e);
            return false;
        }
        return true;
    }

    private static boolean uploadToS3(String bucket, String key, File file)
    {
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try
        {
            LOGGER.info(String.format("bucket[%s], key[%s/%s], file[%s]", bucket, key, file.getName(), file));
            s3Client.putObject(bucket, key + "/" + file.getName(), file);
        }
        catch (AmazonServiceException e)
        {
            LOGGER.error("An error occurred making the upload request or handling response", e);
            return false;
        }
        catch (AmazonClientException e)
        {
            LOGGER.error("An error occurred processing the upload request", e);
            return false;
        }
        return true;
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

    private static String createS3ParsedDataDirName()
    {
        return String.format("product-data/bestbuy/parsed-data/%s", createDatePrefix());
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
                    LOGGER.info(String.format("Progress: %.2f%%", 100.0 * totalBytesRead / totalBytesToDownload));
                    TimeUnit.SECONDS.sleep(5);
                }
            }
            catch (InterruptedException e)
            {
                LOGGER.warn("Exception sleeping in DownloadProgress", e);
            }
        }
    }
}
