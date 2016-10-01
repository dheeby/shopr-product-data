package shopr.productdata.dataloaders;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import shopr.productdata.objects.BestBuyProduct;
import shopr.productdata.objects.Phase;
import shopr.productdata.utils.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by Neil on 9/4/2016.
 * @author Neil Allison
 */
public class BestBuyDataPipeline
{
    public static final String PIPELINE_NAME = "BESTBUY";

    private static final Logger LOGGER = Logger.getLogger(BestBuyDataPipeline.class);
    private static long totalBytesRead;
    private static MySQLHandler mySQLHandler;

    public static boolean executeBestBuyDataPipeline(Phase phase)
    {
        switch (phase)
        {
            case ALL_PHASES:
            case DATA_RETRIEVAL_PHASE:
            case DATA_SANITIZATION_PHASE:
            case DATA_S3_UPLOAD_PHASE:
            case DATA_DB_INSERTION_PHASE:
        }
        return true;
    }

    public static boolean executeBestBuyDataPipeline()
    {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Executing BestBuy data pipeline...");
        String destinationDir = PropertiesLoader.getInstance().getProperty("dir.tmp.dst.bestbuy");

        // Delete directory in case it is still there from previous failed execution
        LocalFileSystemHandler.deleteDirectory(destinationDir);
        if (Files.notExists(Paths.get(destinationDir)))
        {
            LocalFileSystemHandler.createDirectory(destinationDir);
        }

        // Phase 1
        String compressedFile = downloadProductData(destinationDir);
        if (compressedFile == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 1);
            insertFailureState(1);
            return false;
        }

        // Phase 2
        String unzippedDir = unzipBulkDataFile(compressedFile, destinationDir);
        if (unzippedDir == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 2);
            insertFailureState(2);
            return false;
        }

        // Phase 3
        String cleanedDir = cleanData(unzippedDir);
        if (cleanedDir == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 3);
            insertFailureState(3);
            return false;
        }

        // Phase 4
        boolean uploadedToS3 = uploadCleanedProductDataToS3(cleanedDir);
        if (!uploadedToS3)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 4);
            insertFailureState(4);
            return false;
        }

        // Phase 5
        boolean insertedInDB = insertProductDataToDB(cleanedDir);
        if (!insertedInDB)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 5);
            insertFailureState(5);
            return false;
        }

        LocalFileSystemHandler.deleteDirectory(destinationDir);

        long elapsedTime = System.currentTimeMillis() - startTime;
        EmailHandler.sendSuccessEmail(PIPELINE_NAME, Utils.formatTime(elapsedTime));
        LOGGER.info("BestBuy data pipeline complete");
        return true;
    }

    public static String downloadProductData(String destinationDir)
    {
        LOGGER.info("Phase 1: Starting BestBuy bulk data download");
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(Utils.getBestBuyProductsApiUrlString());
        HttpResponse httpResponse;
        try
        {
            httpResponse = httpClient.execute(request);
        }
        catch (IOException e)
        {
            LOGGER.error("Exception executing BestBuy bulk data download request", e);
            return null;
        }

        StatusLine statusLine = httpResponse.getStatusLine();

        if (statusLine.getStatusCode() != 200)
        {
            LOGGER.error("BestBuy bulk data API request did not succeed: " + statusLine);
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
            LOGGER.error("Exception attempting to create output stream for BestBuy bulk data file", e);
            return null;
        }
        catch (IOException e)
        {
            LOGGER.error("Exception retrieving/writing BestBuy bulk data load response content", e);
            return null;
        }
        LOGGER.info("Finished BestBuy bulk data download");
        File uploadFile = new File(bulkDataFilePath);
        String uploadFilename = uploadFile.getName();
        LOGGER.info("Uploading compressed bulk data file to S3: " + uploadFilename);

        if (!S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET, "product-data/bestbuy/bulk-data/" + uploadFilename, uploadFile))
        {
            LOGGER.warn("Bulk data upload to S3 failed");
        }
        else
        {
            LOGGER.info("Success");
        }
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

                try(FileOutputStream fos = new FileOutputStream(currentFile))
                {
                    int bytesRead;
                    while ((bytesRead = zis.read(buffer)) > 0)
                    {
                        fos.write(buffer, 0, bytesRead);
                    }
                    fos.flush();
                    fos.close();
                }
                catch (IOException e)
                {
                    LOGGER.error("Exception unzipping BestBuy bulk data file.", e);
                }
                finally
                {
                    zipEntry = zis.getNextEntry();
                }
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Exception unzipping BestBuy bulk data file.", e);
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOGGER.info(String.format("Unzipping complete. Elapsed Time: %s", Utils.formatTime(elapsedTime)));

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

        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = csvMapper.schemaFor(BestBuyProduct.class);
        schema = schema.withColumnSeparator(',');

        JSONParser jsonParser = new JSONParser();

        File dataDir = new File(dataDirectory);
        File[] dataFiles = dataDir.listFiles();
        if (dataFiles == null)
        {
            LOGGER.error("Failed to get data files from data directory: " + dataDirectory);
            return null;
        }

        File outputDir = new File(PropertiesLoader.getInstance().getProperty("dir.tmp.dst.bestbuy") + File.separator + "cleaned");
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
            List<BestBuyProduct> productsList = new ArrayList<>();
            String dataFilePath = dataFile.getAbsolutePath();
            LOGGER.info("Parsing data file: " + dataFilePath);
            try
            {
                FileReader reader = new FileReader(dataFile);
                JSONArray productDataArray = (JSONArray) jsonParser.parse(reader);
                for (Object productObject : productDataArray)
                {
                    JSONObject product = (JSONObject) productObject;

                    BestBuyProduct bestBuyProduct = new BestBuyProduct();
                    bestBuyProduct.setUpc((String) product.get("upc"));
                    bestBuyProduct.setProductId((Long) product.get("productId"));
                    bestBuyProduct.setName((String) product.get("name"));
                    bestBuyProduct.setType((String) product.get("type"));
                    bestBuyProduct.setRegularPrice((Double) product.get("regularPrice"));
                    bestBuyProduct.setSalePrice((Double) product.get("salePrice"));
                    bestBuyProduct.setOnSale((Boolean) product.get("onSale"));
                    bestBuyProduct.setImage((String) product.get("image"));
                    bestBuyProduct.setThumbnailImage((String) product.get("thumbnailImage"));
                    String shortDescription = (String) product.get("shortDescription");
                    if (shortDescription != null)
                    {
                        shortDescription = shortDescription.replaceAll("\n", " ").replaceAll("\r", " ");
                    }
                    bestBuyProduct.setShortDescription(shortDescription);
                    String longDescription = (String) product.get("longDescription");
                    if (longDescription != null)
                    {
                        longDescription = longDescription.replaceAll("\n", " ").replaceAll("\r", " ");
                    }
                    bestBuyProduct.setLongDescription(longDescription);
                    bestBuyProduct.setCustomerReviewCount((Long) product.get("customerReviewCount"));
                    bestBuyProduct.setCustomerReviewAverage((String) product.get("customerReviewAverage"));
                    bestBuyProduct.setDs(Date.valueOf(Utils.createFormattedDateString()));
                    bestBuyProduct.setPipelineName(BestBuyDataPipeline.PIPELINE_NAME);

                    productsList.add(bestBuyProduct);
                }
                reader.close();
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

            String outFilePath = createCleanedDataFilePath(dataFile.getName().replace(".json", ".csv"));
            File outFile = new File(outFilePath);
            ObjectWriter objectWriter = csvMapper.writer(schema);
            try (FileOutputStream fos = new FileOutputStream(outFile);
                BufferedOutputStream bos = new BufferedOutputStream(fos, 8192);
                OutputStreamWriter osw = new OutputStreamWriter(bos, "UTF-8"))
            {
                objectWriter.writeValue(osw, productsList);
            }
            catch (FileNotFoundException e)
            {
                LOGGER.warn("Could not find file: " + outFilePath, e);
            }
            catch (IOException e)
            {
                LOGGER.error("Exception while writing products to file: " + outFilePath, e);
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
            if (!S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET, "product-data/bestbuy/parsed-data/" + dataFilename, productDataFile))
            {
                LOGGER.warn("Upload failure");
            }
            else
            {
                LOGGER.info("Success");
            }
        }

        return true;
    }

    private static boolean insertProductDataToDB(String productDataDirectory)
    {
        LOGGER.info("Phase 5: Inserting product data into database");

        File[] productDataFiles = (new File(productDataDirectory)).listFiles();
        if (productDataFiles == null || productDataFiles.length == 0)
        {
            LOGGER.warn("No files available to insert");
            return false;
        }

        mySQLHandler = new MySQLHandler();
        for (File productDataFile : productDataFiles)
        {
            String absolutePath = productDataFile.getAbsolutePath();
            LOGGER.info("Inserting product data file: " + absolutePath);
            mySQLHandler.loadDataLocalInfileCsv(absolutePath.replaceAll("\\\\", "\\\\\\\\"));
        }

        LOGGER.info("All product data files to insert to DB complete.");
        return true;
    }

    private static String createUnzippedDataFilePath(String outputBaseDir, String filename)
    {
        return String.format("%s%s%s%s%s", outputBaseDir, File.separator, "unzipped", File.separator, filename);
    }

    private static String createCleanedDataFilePath(String filename)
    {
        return String.format("%s%s%s%s%s%s", PropertiesLoader.getInstance().getProperty("dir.tmp.dst.bestbuy"),
                File.separator, "cleaned", File.separator, "cleaned_", filename);
    }

    private static String createCompressedProductDataFilename()
    {
        return String.format("%s_products_BestBuy.json.zip", Utils.createFormattedDateString());
    }

    private static void insertFailureState(int phase)
    {
        if (mySQLHandler == null)
        {
            mySQLHandler = new MySQLHandler();
        }

        mySQLHandler.insertFailureState(PIPELINE_NAME, phase, new Date(System.currentTimeMillis()));
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
