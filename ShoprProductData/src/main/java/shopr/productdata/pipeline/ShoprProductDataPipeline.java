package shopr.productdata.pipeline;

import org.apache.log4j.Logger;
import shopr.productdata.objects.Phase;

import java.util.ArrayList;

/**
 * Created by Neil on 10/4/2016.
 *
 * @author Neil Allison
 */
public class ShoprProductDataPipeline
{
    private static final Logger LOGGER = Logger.getLogger(ShoprProductDataPipeline.class);

    public static void executeShoprProductDataPipeline(ArrayList<String[]> states) throws IllegalArgumentException
    {
        for (String[] state : states)
        {
            Phase phase = Phase.valueOf(state[1]);
            switch(state[0])
            {
                case BestBuyDataPipeline.PIPELINE_NAME:
                    if (!phase.name().equals(Phase.NONE.name()) && !BestBuyDataPipeline.executeBestBuyDataPipeline(phase))
                    {
                        LOGGER.error("BestBuyDataPipeline failed. Check failure email and logs.");
                    }
                    break;
                case WalMartDataPipeline.PIPELINE_NAME:
                    if (!phase.name().equals(Phase.NONE.name()) && !WalMartDataPipeline.executeWalMartDataPipeline(phase))
                    {
                        LOGGER.error("WalMartDataPipeline failed. Check failure email and logs.");
                    }
                    break;
            }
        }
    }

}
