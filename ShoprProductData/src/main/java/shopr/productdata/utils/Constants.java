package shopr.productdata.utils;

/**
 * Created by Neil on 9/4/2016.
 *
 * @author Neil Allison
 */
public class Constants {
    public static final String BESTBUY_BULK_PRODUCT_API_BASE = "https://api.bestbuy.com/v1/products.json.zip?apiKey=";
    public static final String WALMART_TAXONOMY_API_BASE = "http://api.walmartlabs.com/v1/taxonomy?apiKey=";
    public static final String WALMART_API_BASE = "http://api.walmartlabs.com";

    public static final boolean ENABLE_BESTBUY_UNCLEANED_S3_UPLOAD = false;
    public static final boolean ENABLE_WALMART_UNCLEANED_S3_UPLOAD = false;
    public static final boolean ENABLE_AMAZON_UNCLEANED_S3_UPLOAD = false;

    public static final String SHOPR_S3_DATA_BUCKET = "shopr-data.com";
    public static String SHOPR_PROPERTIES_FILE = "shopr-production.properties";
}
