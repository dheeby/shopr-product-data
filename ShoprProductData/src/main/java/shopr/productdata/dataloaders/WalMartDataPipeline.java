package shopr.productdata.dataloaders;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import shopr.productdata.objects.WalMartTaxonomyTreeCategory;
import shopr.productdata.objects.WalMartTaxonomyTree;
import shopr.productdata.utils.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Neil on 9/4/2016.
 * @author Neil Allison
 */
public class WalMartDataPipeline
{
    public static final String PIPELINE_NAME = "WALMART";

    private static final Logger LOGGER = Logger.getLogger(WalMartDataPipeline.class);
    private static final String TAXONOMY_TREE_FILENAME = "walmart_taxonomy.json";

    public static boolean executeWalMartDataPipeline()
    {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Executing WalMart data pipeline...");
        String destinationDir = PropertiesLoader.getInstance().getProperty("dir.tmp.dst.walmart");
        if (Files.notExists(Paths.get(destinationDir)))
        {
            LocalFileSystemHandler.createDirectory(destinationDir);
        }

        // Phase 1
        String taxonomyTreeFilePath = downloadTaxonomyTree(destinationDir);
        if (taxonomyTreeFilePath == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 1);
            // TODO: insert state into state table
            return false;
        }

        // Phase 2
        WalMartTaxonomyTree taxonomyTree = parseTaxonomyTree(taxonomyTreeFilePath);
        if (taxonomyTree.getCategories() == null)
        {
            EmailHandler.sendFailureEmail(PIPELINE_NAME, 2);
            // TODO: insert state into state table
            return false;
        }

        // TODO: add back in when pipeline phases complete
//        LocalFileSystemHandler.deleteDirectory(destinationDir);

        long elapsedTime = System.currentTimeMillis() - startTime;
        EmailHandler.sendSuccessEmail(PIPELINE_NAME, Utils.formatTime(elapsedTime));
        LOGGER.info("WalMart data pipeline complete");
        return true;
    }

    public static String downloadTaxonomyTree(String destinationDir)
    {
        LOGGER.info("Phase 1: Starting WalMart taxonomy tree download");
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
        return taxonomyTreeFilePath;
    }

    public static WalMartTaxonomyTree parseTaxonomyTree(String taxonomyTreeFilePath)
    {
        WalMartTaxonomyTree taxonomyTree = new WalMartTaxonomyTree();
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        JSONParser jsonParser = new JSONParser();

        try
        {
            JSONObject categoriesObject = (JSONObject) jsonParser.parse(new FileReader(new File(taxonomyTreeFilePath)));
            JSONArray categories = (JSONArray) categoriesObject.get("categories");

            List<WalMartTaxonomyTreeCategory> categoriesList = traverseTree(categories);
            taxonomyTree.setCategories(categoriesList);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }

        System.out.print(taxonomyTree.toString());

        return taxonomyTree;
    }

    private static List<WalMartTaxonomyTreeCategory> traverseTree(JSONArray categoriesTree)
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

    private static String getTaxonomyApiUrlString()
    {
        return Constants.WALMART_TAXONOMY_API_BASE + PropertiesLoader.getInstance().getProperty("walmart.apikey");
    }
}
