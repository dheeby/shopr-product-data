package shopr.productdata.pipeline;

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
import shopr.productdata.objects.ShoprProduct;
import shopr.productdata.objects.PipelineName;
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
 *
 * @author Neil Allison
 */
public class BestBuyDataPipeline extends DataPipeline
{
    private static long totalBytesRead;

    public BestBuyDataPipeline(PipelineName pipelineName)
    {
        super(pipelineName);
        this.LOGGER = Logger.getLogger(this.getClass());
    }

    protected boolean executeDataRetrievalPhase(String destinationDir)
    {
        LOGGER.info("Phase 1: Starting BestBuy data retrieval");
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
            return false;
        }

        StatusLine statusLine = httpResponse.getStatusLine();

        if (statusLine.getStatusCode() != 200)
        {
            LOGGER.error("BestBuy bulk data API request did not succeed: " + statusLine);
            return false;
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
            totalBytesRead = 0;
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
            return false;
        }
        catch (IOException e)
        {
            LOGGER.error("Exception retrieving/writing BestBuy bulk data load response content", e);
            return false;
        }
        LOGGER.info("Finished BestBuy bulk data download");
        File uploadFile = new File(bulkDataFilePath);
        String uploadFilename = uploadFile.getName();
        LOGGER.info("Uploading compressed bulk data file to S3: " + uploadFilename);

        if (Constants.ENABLE_BESTBUY_UNCLEANED_S3_UPLOAD && !S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET,
                "product-data/bestbuy/bulk-data/" + uploadFilename, uploadFile))
        {
            LOGGER.warn("Bulk data upload to S3 failed");
        }

        return true;
    }

    protected boolean executePreProcessPhase(String destinationDir)
    {
        String compressedFilePath;
        if ((compressedFilePath = getCompressedBulkDataFilePath(destinationDir)) == null)
        {
            LOGGER.error("Could not get file path for compressed bulk data file.");
            return false;
        }
        LOGGER.info("Phase 2: Unzipping BestBuy bulk data file: " + compressedFilePath);
        long startTime = System.currentTimeMillis();

        File outputDir = new File(destinationDir + File.separator + "unzipped");
        if (!outputDir.exists())
        {
            LOGGER.info("Creating unzipped output directory: " + destinationDir);
            if (!outputDir.mkdir())
            {
                LOGGER.error("Failed to create unzipped output directory");
                return false;
            }
        }

        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(compressedFilePath)))
        {
            ZipEntry zipEntry = zis.getNextEntry();
            byte[] buffer = new byte[8192];

            while (zipEntry != null)
            {
                String filename = zipEntry.getName();
                File currentFile = new File(createUnzippedDataFilePath(filename));
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
            Files.delete(Paths.get(compressedFilePath));
            LOGGER.info("Compressed bulk data file deleted");
        }
        catch (IOException e)
        {
            LOGGER.warn("Failed to delete compressed data file");
        }
        return true;
    }

    protected boolean executeSanitizationPhase(String dataDirectory)
    {
        LOGGER.info("Phase 3: Starting data clean phase for data directory: " + dataDirectory);

        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = csvMapper.schemaFor(ShoprProduct.class);
        schema = schema.withColumnSeparator(',');

        JSONParser jsonParser = new JSONParser();

        File dataDir = new File(dataDirectory);
        File[] dataFiles = dataDir.listFiles();
        if (dataFiles == null)
        {
            LOGGER.error("Failed to get data files from data directory: " + dataDirectory);
            return false;
        }

        File outputDir = new File(cleanedDir);
        if (!outputDir.exists())
        {
            LOGGER.info("Creating BestBuy cleaned output directory: " + outputDir.getAbsolutePath());
            if (!outputDir.mkdir())
            {
                LOGGER.error("Failed to BestBuy create cleaned output directory");
                return false;
            }
        }

        for (File dataFile : dataFiles)
        {
            List<ShoprProduct> productsList = new ArrayList<>();
            String dataFilePath = dataFile.getAbsolutePath();
            LOGGER.info("Parsing data file: " + dataFilePath);
            try
            {
                FileReader reader = new FileReader(dataFile);
                JSONArray productDataArray = (JSONArray) jsonParser.parse(reader);
                Date ds = Date.valueOf(Utils.createFormattedDateString());
                for (Object productObject : productDataArray)
                {
                    JSONObject product = (JSONObject) productObject;

                    ShoprProduct shoprProduct = new ShoprProduct();
                    shoprProduct.setDs(ds);
                    shoprProduct.setUpc((String) product.get("upc"));
                    shoprProduct.setProductId((Long) product.get("productId"));
                    String name = (String) product.get("name");
                    if (name != null)
                    {
                        name = name.replaceAll("\\r?\\n", " ");
                    }
                    shoprProduct.setName(name);
                    String type = (String) product.get("type");
                    if (type != null)
                    {
                        type = type.replaceAll("\\r?\\n", " ");
                    }
                    shoprProduct.setType(type);
                    shoprProduct.setRegularPrice((Double) product.get("regularPrice"));
                    shoprProduct.setSalePrice((Double) product.get("salePrice"));
                    shoprProduct.setOnSale((Boolean) product.get("onSale"));
                    shoprProduct.setImage((String) product.get("image"));
                    shoprProduct.setThumbnailImage((String) product.get("thumbnailImage"));
                    String shortDescription = (String) product.get("shortDescription");
                    if (shortDescription != null)
                    {
                        shortDescription = shortDescription.replaceAll("\\r?\\n", " ");
                    }
                    shoprProduct.setShortDescription(shortDescription);
                    String longDescription = (String) product.get("longDescription");
                    if (longDescription != null)
                    {
                        longDescription = longDescription.replaceAll("\\r?\\n", " ");
                    }
                    shoprProduct.setLongDescription(longDescription);
                    shoprProduct.setCustomerReviewCount((Long) product.get("customerReviewCount"));
                    shoprProduct.setCustomerReviewAverage((String) product.get("customerReviewAverage"));
                    shoprProduct.setPipelineName(pipelineName.name());
                    shoprProduct.setCategoryPath(((JSONArray)product.get("categoryPath")).toJSONString());

                    productsList.add(shoprProduct);
                }
                reader.close();
            }
            catch (ParseException e)
            {
                LOGGER.error("Parsing data file failed: " + dataFilePath, e);
            }
            catch (IOException e)
            {
                LOGGER.error("Opening data file failed: " + dataFilePath, e);
                return false;
            }

            String outFilePath = createCleanedDataFilePath(dataFile.getName().replace(".json", ".csv"));
            File outFile = new File(outFilePath);
            ObjectWriter objectWriter = csvMapper.writer(schema);
            try (
                    FileOutputStream fos = new FileOutputStream(outFile);
                    BufferedOutputStream bos = new BufferedOutputStream(fos, 8192);
                    OutputStreamWriter osw = new OutputStreamWriter(bos, "UTF-8")
            )
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

        return true;
    }

    protected boolean executeS3UploadPhase(String productDataDirectory)
    {
        LOGGER.info("Phase 4: Uploading product data to S3");

        String zipFileName = Utils.createFormattedDateString() + "_parsed-data.zip";
        String zipFilePath = baseDir + File.separator + zipFileName;

        try
        {
            boolean zipFiles = LocalFileSystemHandler.zipFiles(zipFilePath, productDataDirectory);
            if (!zipFiles)
            {
                LOGGER.error("Compressing data files to zip directory failed.");
                return false;
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Error occurred writing or reading file", e);
        }

        if (!S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET, "product-data/bestbuy/parsed-data/" +
                Utils.createFormattedDateString() + "/" + zipFileName, new File(zipFilePath)))
        {
            LOGGER.error("S3 parsed data upload failure");
            return false;
        }

        return true;
    }

    protected boolean executeDbInsertionPhase(String productDataDirectory)
    {
        LOGGER.info("Phase 5: Inserting product data into database");

        File[] productDataFiles = (new File(productDataDirectory)).listFiles();
        if (productDataFiles == null || productDataFiles.length == 0)
        {
            LOGGER.error("No files available to insert");
            return false;
        }

        MySQLHandler mySQLHandler = new MySQLHandler();
        for (File productDataFile : productDataFiles)
        {
            String absolutePath = productDataFile.getAbsolutePath();
            LOGGER.info("Inserting product data file: " + absolutePath);
            mySQLHandler.loadDataLocalInfileCsv(absolutePath.replaceAll("\\\\", "\\\\\\\\"));
        }

        LOGGER.info("All product data files to insert to DB complete.");
        return true;
    }

    private String createUnzippedDataFilePath(String filename)
    {
        return String.format("%s%s%s", uncleanedDir, File.separator, filename);
    }

    private String createCleanedDataFilePath(String filename)
    {
        return String.format("%s%s%s%s", cleanedDir, File.separator, "cleaned_", filename);
    }

    private String createCompressedProductDataFilename()
    {
        return String.format("%s_products_BestBuy.json.zip", Utils.createFormattedDateString());
    }

    private String getCompressedBulkDataFilePath(String baseDir)
    {
        File[] files = (new File(baseDir)).listFiles();
        if (files == null)
        {
            return null;
        }
        for (File f : files)
        {
            if (f.getName().endsWith(".zip"))
            {
                return f.getAbsolutePath();
            }
        }
        return null;
    }

    private class DownloadProgress extends Thread
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
