package shopr.productdata.utils;

import shopr.productdata.ShoprProductDataDriver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Neil on 9/14/2016.
 */
public final class PropertiesLoader
{
    private Properties properties;

    private PropertiesLoader()
    {
        try (InputStream is = ShoprProductDataDriver.class.getClassLoader().getResourceAsStream(Constants.SHOPR_PROPERTIES_FILE))
        {
            properties = new Properties();
            if (is != null)
            {
                properties.load(is);
            }
            else
            {
                throw new FileNotFoundException("Property file not found: " + Constants.SHOPR_PROPERTIES_FILE);
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static class PropertiesLoaderHolder
    {
        private static final PropertiesLoader INSTANCE = new PropertiesLoader();
    }

    public static PropertiesLoader getInstance()
    {
        return PropertiesLoaderHolder.INSTANCE;
    }

    public String getProperty(String propertyKey)
    {
        return properties.getProperty(propertyKey);
    }
}
