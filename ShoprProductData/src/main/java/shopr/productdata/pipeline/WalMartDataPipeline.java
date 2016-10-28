package shopr.productdata.pipeline;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import shopr.productdata.objects.*;
import shopr.productdata.utils.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Neil on 9/4/2016.
 *
 * @author Neil Allison
 */
public class WalMartDataPipeline extends DataPipeline
{
    private static final String TAXONOMY_TREE_FILENAME = "walmart_taxonomy.json";

    private String[] categories = {
            "4044_90548",           // Appliances
            "3944_1229722",         // Apple Brand Experience
            "3944_1156273_1156275", // iPad Air
            "3944_542371",          // Cell Phones
            "3944_3951",            // Computers
            "3944_1060825_447913",  // All TVs
            "3944_1229723_5635313", // Fitbit
            "3944_133277",          // Cameras and Camcorders
            "3944_3951_1230331",    // Computer monitors
            "3944_1228606",         // Drones
            "3944_1078524",         // iPad and Tablets
    };

    public WalMartDataPipeline(PipelineName pipelineName)
    {
        super(pipelineName);
        this.LOGGER = Logger.getLogger(this.getClass());
    }

    public boolean executeDataRetrievalPhase(String destinationDir)
    {
        LOGGER.info("Phase 1: Starting data retrieval");
        HttpClient httpClient = HttpClientBuilder.create().build();
        String apiSuffix = String.format("/v1/paginated/items?apiKey=%s&format=json&category=",
                PropertiesLoader.getInstance().getProperty("walmart.apikey"));
        int numPages = 400;

        for (String category : categories)
        {
            int pageNumber = 0;
            String requestUrl = Constants.WALMART_API_BASE + apiSuffix + category;
            String nextPageSuffix = "";
            for (int i = 0; i < numPages; i++)
            {
                if (nextPageSuffix == null)
                {
                    break;
                }
                HttpGet request = new HttpGet(requestUrl);
                HttpResponse httpResponse;
                try
                {
                    httpResponse = httpClient.execute(request);
                }
                catch (IOException e)
                {
                    LOGGER.error("Exception executing WalMart paginated products download request", e);
                    return false;
                }

                StatusLine statusLine = httpResponse.getStatusLine();

                if (statusLine.getStatusCode() != 200)
                {
                    if (statusLine.getStatusCode() == 504)
                    {
                        LOGGER.warn("Request timed out: " + statusLine);
                        continue;
                    }
                    else
                    {
                        LOGGER.error("WalMart Paginated Products API request did not succeed: " + statusLine);
                        return false;
                    }
                }

                String dataFilename = category + "_page_" + (pageNumber++) + ".json";
                String dataFilePath = Paths.get(uncleanedDir, dataFilename).toString();
                try (
                        InputStream is = httpResponse.getEntity().getContent();
                        FileOutputStream fos = new FileOutputStream(dataFilePath)
                )
                {
                    LOGGER.info("Downloading data to file: " + dataFilePath);
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while((bytesRead = is.read(buffer)) > 0)
                    {
                        fos.write(buffer, 0, bytesRead);
                    }
                    fos.flush();
                    fos.close();
                }
                catch (IOException e)
                {
                    LOGGER.error("Exception retrieving/writing WalMart paginated products response content", e);
                    return false;
                }

                try
                {
                    JSONParser jsonParser = new JSONParser();
                    FileReader reader = new FileReader(new File(dataFilePath));
                    JSONObject productsDataObject = (JSONObject) jsonParser.parse(reader);
                    nextPageSuffix = (String) productsDataObject.get("nextPage");
                    requestUrl = Constants.WALMART_API_BASE + nextPageSuffix;
                }
                catch (FileNotFoundException e)
                {
                    LOGGER.warn("Could not find data file: " + dataFilePath);
                }
                catch (ParseException e)
                {
                    LOGGER.warn("Parsing JSON failed while retrieving next results page.", e);
                }
                catch (IOException e)
                {
                    LOGGER.warn("Reading data file failed: " + dataFilePath);
                }
            }
        }

        LOGGER.info("Uploading uncleaned data to S3");

        String zipFileName = Utils.createFormattedDateString() + "_uncleaned-data.zip";
        String zipFilePath = baseDir + File.separator + zipFileName;

        try
        {
            LocalFileSystemHandler.zipFiles(zipFilePath, uncleanedDir);
        }
        catch (IOException e)
        {
            LOGGER.warn("Compressing uncleaned data files to zip directory failed: " + zipFilePath + ", " + destinationDir);
        }

        if (Constants.ENABLE_WALMART_UNCLEANED_S3_UPLOAD && !S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET,
                "product-data/walmart/uncleaned-data/" + zipFileName, new File(zipFilePath)))
        {
            LOGGER.warn("S3 uncleaned data upload failure");
        }

        return true;
    }

    protected boolean executePreProcessPhase(String dataDirectory)
    {
        LOGGER.info(String.format("PREPROCESS phase is not used with the %s data pipeline.", pipelineName.name()));
        return true;
    }

    protected boolean executeSanitizationPhase(String dataDirectory)
    {
        LOGGER.info("Phase 3: Starting data sanitization");

        CsvMapper csvMapper = new CsvMapper();

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
            LOGGER.info("Creating WalMart cleaned output directory: " + outputDir.getAbsolutePath());
            if (!outputDir.mkdir())
            {
                LOGGER.error("Failed to create WalMart cleaned output directory");
                return false;
            }
        }

        for (File dataFile : dataFiles)
        {
            List<ShoprProductPrice> productPriceList = new ArrayList<>();
            List<ShoprProductInfo> productInfoList = new ArrayList<>();
            String dataFilePath = dataFile.getAbsolutePath();
            LOGGER.info("Parsing data file: " + dataFilePath);
            try
            {
                FileReader reader = new FileReader(dataFile);
                JSONObject productDataFileObject = (JSONObject) jsonParser.parse(reader);
                if (productDataFileObject.size() == 0)
                {
                    LOGGER.warn("The data file contained no items: " + dataFilePath);
                    reader.close();
                    continue;
                }
                JSONArray productDataArray = (JSONArray) productDataFileObject.get("items");
                Date ds = Date.valueOf(Utils.createFormattedDateString());
                for (Object productObject : productDataArray)
                {
                    JSONObject product = (JSONObject) productObject;

                    String upc= (String) product.get("upc");
                    if (upc == null)
                    {
                        continue;
                    }

                    /* Data for table product_price */
                    ShoprProductPrice shoprProductPrice = new ShoprProductPrice();
                    shoprProductPrice.setDs(ds);
                    shoprProductPrice.setUpc(upc);
                    Double salePrice = (Double) product.get("salePrice");
                    if (salePrice == null)
                    {
                        salePrice = -0.01;
                    }
                    Double regularPrice = (Double) product.get("msrp");
                    if (regularPrice == null)
                    {
                        regularPrice = salePrice;
                    }
                    shoprProductPrice.setRegularPrice(regularPrice);
                    shoprProductPrice.setSalePrice(salePrice);
                    shoprProductPrice.setVendor(pipelineName.name());

                    /* Data for table product_info */
                    ShoprProductInfo shoprProductInfo = new ShoprProductInfo();
                    shoprProductInfo.setUpc(upc);
                    String name = (String) product.get("name");
                    if (name != null)
                    {
                        shoprProductInfo.setName(name.replaceAll("\\r?\\n", " "));
                    }
                    String image = (String) product.get("image");
                    if (image != null)
                    {
                        shoprProductInfo.setImage(image.replace("\"", ""));
                    }
                    String thumbnailImage = (String) product.get("thumbnailImage");
                    if (thumbnailImage != null)
                    {
                        shoprProductInfo.setThumbnail(thumbnailImage.replace("\"", ""));
                    }
                    String shortDescription = (String) product.get("shortDescription");
                    if (shortDescription != null)
                    {
                        shortDescription = shortDescription.replaceAll("\\r?\\n", " ");
                    }
                    shoprProductInfo.setShortDescription(shortDescription);
                    String longDescription = (String) product.get("longDescription");
                    if (longDescription != null)
                    {
                        longDescription = longDescription.replaceAll("\\r?\\n", " ");
                    }
                    shoprProductInfo.setLongDescription(longDescription);
                    shoprProductInfo.setCustomerReviewCount(0L);
                    shoprProductInfo.setCustomerReviewAverage(null);
                    shoprProductInfo.setVendor(pipelineName.name());
                    shoprProductInfo.setCategoryPath((String) product.get("categoryPath"));

                    productPriceList.add(shoprProductPrice);
                    productInfoList.add(shoprProductInfo);
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

            String filename = dataFile.getName().replace(".json", ".nsv");
            String priceOutFilePath = createCleanedProductPriceDataFilePath(filename);
            File outFile = new File(priceOutFilePath);
            CsvSchema schema = csvMapper.schemaFor(ShoprProductPrice.class);
            schema = schema.withColumnSeparator('\0');
            ObjectWriter objectWriter = csvMapper.writer(schema);
            try (
                    FileOutputStream fos = new FileOutputStream(outFile);
                    BufferedOutputStream bos = new BufferedOutputStream(fos, 8192);
                    OutputStreamWriter osw = new OutputStreamWriter(bos, "UTF-8")
            )
            {
                objectWriter.writeValue(osw, productPriceList);
            }
            catch (FileNotFoundException e)
            {
                LOGGER.warn("Could not find file: " + priceOutFilePath, e);
            }
            catch (IOException e)
            {
                LOGGER.error("Exception while writing products to file: " + priceOutFilePath, e);
            }

            String infoOutFilePath = createCleanedProductInfoDataFilePath(filename);
            outFile = new File(infoOutFilePath);
            schema = csvMapper.schemaFor(ShoprProductInfo.class);
            schema = schema.withColumnSeparator('\0');
            objectWriter = csvMapper.writer(schema);
            try (
                    FileOutputStream fos = new FileOutputStream(outFile);
                    BufferedOutputStream bos = new BufferedOutputStream(fos, 8192);
                    OutputStreamWriter osw = new OutputStreamWriter(bos, "UTF-8")
            )
            {
                objectWriter.writeValue(osw, productInfoList);
            }
            catch (FileNotFoundException e)
            {
                LOGGER.warn("Could not find file: " + infoOutFilePath, e);
            }
            catch (IOException e)
            {
                LOGGER.error("Exception while writing products to file: " + infoOutFilePath, e);
            }
        }

        return true;
    }

    protected boolean executeS3UploadPhase(String dataDirectory)
    {
        LOGGER.info("Phase 4: Starting S3 upload");

        String zipPriceFileName = Utils.createFormattedDateString() + "_price-parsed-data.zip";
        String zipPriceFilePath = baseDir + File.separator + zipPriceFileName;

        String zipInfoFileName = Utils.createFormattedDateString() + "_info-parsed-data.zip";
        String zipInfoFilePath = baseDir + File.separator + zipInfoFileName;

        return s3Upload(zipPriceFileName, zipPriceFilePath, dataDirectory + File.separator + "product_price")
                && s3Upload(zipInfoFileName, zipInfoFilePath, dataDirectory + File.separator + "product_info");
    }

    protected boolean executeDbInsertionPhase(String dataDirectory)
    {
        LOGGER.info("Phase 5: Starting DB insertion");

        return dbInsert(dataDirectory + File.separator + "product_info", "product_info")
                && dbInsert(dataDirectory + File.separator + "product_price", "product_prices");
    }

    private boolean s3Upload(String zipFileName, String zipFilePath, String productDataDirectory)
    {
        try
        {
            if (!LocalFileSystemHandler.zipFiles(zipFilePath, productDataDirectory))
            {
                LOGGER.error("Compressing data files to zip directory failed.");
                return false;
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Error occurred writing or reading file", e);
            return false;
        }

        if (!S3Handler.uploadToS3(Constants.SHOPR_S3_DATA_BUCKET, "product-data/bestbuy/parsed-data/" +
                Utils.createFormattedDateString() + "/" + zipFileName, new File(zipFilePath)))
        {
            LOGGER.error("S3 parsed data upload failure");
            return false;
        }
        return true;
    }

    private boolean dbInsert(String dir, String table)
    {
        File[] productDataFiles = (new File(dir)).listFiles();
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
            mySQLHandler.loadDataLocalInfileCsv(absolutePath.replaceAll("\\\\", "\\\\\\\\"), table);
        }

        LOGGER.info("All product data files to insert to DB complete.");
        return true;
    }

    private String createCleanedProductPriceDataFilePath(String filename)
    {
        return String.format("%s%s%s%s%s%s", cleanedDir, File.separator, "product_price", File.separator, "price_", filename);
    }

    private String createCleanedProductInfoDataFilePath(String filename)
    {
        return String.format("%s%s%s%s%s%s", cleanedDir, File.separator, "product_info", File.separator, "info_", filename);
    }

    @SuppressWarnings("unused")
    protected WalMartTaxonomyTree downloadTaxonomyTree(String destinationDir)
    {
        LOGGER.info("Starting WalMart taxonomy tree download");
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(getTaxonomyApiUrlString());
        HttpResponse httpResponse;
        try
        {
            httpResponse = httpClient.execute(request);
        }
        catch (IOException e)
        {
            LOGGER.error("Exception executing WalMart taxonomy tree download request", e);
            return null;
        }

        LOGGER.info(httpResponse.getStatusLine());

        String taxonomyTreeFilename = TAXONOMY_TREE_FILENAME;
        String taxonomyTreeFilePath = Paths.get(destinationDir, taxonomyTreeFilename).toString();

        try (
                InputStream is = httpResponse.getEntity().getContent();
                FileOutputStream fos = new FileOutputStream(taxonomyTreeFilePath)
        )
        {
            LOGGER.info("Writing WalMart taxonomy tree to file...");
            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = is.read(buffer)) > 0)
            {
                fos.write(buffer, 0, bytesRead);
            }

            fos.flush();
            fos.close();
        }
        catch (FileNotFoundException e)
        {
            LOGGER.error("Exception attempting to create output stream for WalMart taxonomy tree file", e);
            return null;
        }
        catch (IOException e)
        {
            LOGGER.error("Exception retrieving/writing WalMart taxonomy tree response content", e);
            return null;
        }
        LOGGER.info("Finished WalMart taxonomy tree download");
        return parseTaxonomyTree(taxonomyTreeFilePath);
    }

    protected WalMartTaxonomyTree parseTaxonomyTree(String taxonomyTreeFilePath)
    {
        WalMartTaxonomyTree taxonomyTree = new WalMartTaxonomyTree();
        JSONParser jsonParser = new JSONParser();

        try
        {
            JSONObject categoriesObject = (JSONObject) jsonParser.parse(new FileReader(new File(taxonomyTreeFilePath)));
            JSONArray categories = (JSONArray) categoriesObject.get("categories");

            List<WalMartTaxonomyTreeCategory> categoriesList = traverseTree(categories);
            taxonomyTree.setCategories(categoriesList);
        }
        catch (IOException | ParseException e)
        {
            e.printStackTrace();
        }

        System.out.print(taxonomyTree.toString());

        return taxonomyTree;
    }

    protected List<WalMartTaxonomyTreeCategory> traverseTree(JSONArray categoriesTree)
    {
        List<WalMartTaxonomyTreeCategory> categories = new ArrayList<>();
        for (Object categoryObject : categoriesTree)
        {
            JSONObject categoryJSONObject = (JSONObject) categoryObject;
            String id = (String) categoryJSONObject.get("id");
            String name = (String) categoryJSONObject.get("name");
            JSONArray children = (JSONArray) categoryJSONObject.get("children");
            WalMartTaxonomyTreeCategory walMartTaxonomyTreeCategory = new WalMartTaxonomyTreeCategory(id, name);
            if (children != null && children.size() > 0)
            {
                walMartTaxonomyTreeCategory.setChildren(traverseTree(children));
            }
            categories.add(walMartTaxonomyTreeCategory);
        }
        return categories;
    }

    private String getTaxonomyApiUrlString()
    {
        return Constants.WALMART_TAXONOMY_API_BASE + PropertiesLoader.getInstance().getProperty("walmart.apikey");
    }
}
