package shopr.productdata.utils;

import org.joda.time.DateTime;

/**
 * Created by Neil on 9/27/2016.
 */
public class Utils
{
    public static String formatTime(long timeInMillis)
    {
        long minutes = timeInMillis / 60000;
        long seconds = (timeInMillis - (60000 * minutes)) / 1000;
        long milliseconds = timeInMillis - (60000 * minutes) - (1000 * seconds);
        return String.format("%dmin, %ds, %dms", minutes, seconds, milliseconds);
    }

    public static String createFormattedDateString()
    {
        DateTime dt = new DateTime();
        return String.format("%04d-%02d-%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
    }

    public static String getBestBuyProductsApiUrlString()
    {
        return Constants.BESTBUY_BULK_PRODUCT_API_BASE + PropertiesLoader.getInstance().getProperty("bestbuy.apikey");
    }
}
