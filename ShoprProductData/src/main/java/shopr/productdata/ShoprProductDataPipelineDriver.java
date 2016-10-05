package shopr.productdata;

import org.apache.log4j.Logger;
import shopr.productdata.objects.PipelineName;
import shopr.productdata.pipeline.ShoprProductDataPipeline;
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
        if (args.length != 3)
        {
            System.err.println("Usage: java ShoprProductDataPipelineDriver [BestBuy Phase] [WalMart Phase] [Max Retries]");
            return;
        }

        MySQLHandler mySQLHandler = new MySQLHandler();
        int numRetries = 0;
        int maxRetries = Integer.parseInt(args[2]);
        ArrayList<String[]> states = new ArrayList<>();
        states.add(new String[]{PipelineName.BESTBUY.name(), args[0], Utils.createFormattedDateString()});
        states.add(new String[]{PipelineName.WALMART.name(), args[1], Utils.createFormattedDateString()});

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
