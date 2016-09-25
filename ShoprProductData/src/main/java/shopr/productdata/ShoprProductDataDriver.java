package shopr.productdata;

import shopr.productdata.dataloaders.BestBuyDataPipeline;
import shopr.productdata.utils.PropertiesLoader;

import java.io.File;

/**
 * Created by Neil on 9/14/2016.
 */
public class ShoprProductDataDriver
{
    /**
     * Entry point for the ShoprProductDataServer.
     *
     * @param args
     */
    public static void main(String[] args)
    {
        String dstDir = PropertiesLoader.getInstance().getProperty("dir.tmp.destination");
        String compressedFile = BestBuyDataPipeline.downloadProductData(dstDir);
//        String compressedFile = "C:\\Users\\Neil\\Desktop\\CS49000\\ShoprProductDataServer\\data\\tmp\\2016-09-25_products_BestBuy.json.zip";
        String unzippedDir = BestBuyDataPipeline.unzipBulkDataFile(compressedFile, dstDir);
        BestBuyDataPipeline.cleanData(unzippedDir);
    }
}
