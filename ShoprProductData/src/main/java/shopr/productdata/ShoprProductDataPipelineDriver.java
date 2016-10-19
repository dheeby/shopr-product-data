package shopr.productdata;

import org.apache.log4j.Logger;
import shopr.productdata.objects.PipelineName;
import shopr.productdata.pipeline.ShoprProductDataPipeline;
import shopr.productdata.utils.Constants;
import shopr.productdata.utils.MySQLHandler;
import shopr.productdata.utils.Utils;

import java.util.ArrayList;

/**
 * Created by Neil on 9/14/2016.
 *
 * @author Neil Allison
 */
public class ShoprProductDataPipelineDriver
{
    private static final Logger LOGGER = Logger.getLogger(ShoprProductDataPipelineDriver.class);

    public static void main(String[] args)
    {
        String usage = "Usage: java ShoprProductDataPipelineDriver [0 for Dev, 1 for Prod] [BestBuy Phase] [WalMart Phase] [Max Retries]";
        if (args.length != 4)
        {
            LOGGER.error(usage);
            return;
        }

        try
        {
            if (Integer.parseInt(args[0]) == 0)
            {
                Constants.SHOPR_PROPERTIES_FILE = "shopr-development.properties";
            } else
            {
                Constants.SHOPR_PROPERTIES_FILE = "shopr-production.properties";
            }
        }
        catch (NumberFormatException e)
        {
            LOGGER.error(usage);
            return;
        }

        MySQLHandler mySQLHandler = new MySQLHandler();
        int numRetries = 0;
        int maxRetries = Integer.parseInt(args[3]);
        ArrayList<String[]> states = new ArrayList<>();
        states.add(new String[]{PipelineName.BESTBUY.name(), args[1], Utils.createFormattedDateString()});
        states.add(new String[]{PipelineName.WALMART.name(), args[2], Utils.createFormattedDateString()});

        try
        {
            do
            {
                ShoprProductDataPipeline.executeShoprProductDataPipeline(states);
            } while((states = mySQLHandler.getPipelineFailureStates()).size() > 0 && numRetries++ < maxRetries);
        }
        catch (IllegalArgumentException e)
        {
            LOGGER.error(String.format("Invalid Phase: %s", e.getMessage()), e);
        }
    }
}
