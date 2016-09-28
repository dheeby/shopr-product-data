package shopr.productdata;

import shopr.productdata.dataloaders.BestBuyDataPipeline;
import shopr.productdata.dataloaders.WalMartDataPipeline;
import shopr.productdata.utils.MySQLHandler;

import java.sql.Date;

/**
 * Created by Neil on 9/14/2016.
 * @author Neil Allison
 */
public class ShoprProductDataDriver
{
    public static void main(String[] args)
    {
        BestBuyDataPipeline.executeBestBuyDataPipeline();
//        WalMartDataPipeline.executeWalMartDataPipeline();
    }
}
