package shopr.productdata;

import shopr.productdata.dataloaders.BestBuyDataPipeline;
import shopr.productdata.utils.PropertiesLoader;

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
        BestBuyDataPipeline.unzipBulkDataFile(compressedFile, dstDir);
    }
}
