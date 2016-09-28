package shopr.productdata.utils;

import org.apache.log4j.Logger;
import shopr.productdata.objects.BestBuyProduct;

import java.sql.*;
import java.util.Properties;

/**
 * Created by Neil on 9/27/2016.
 */
public class MySQLHandler
{
    private static final Logger LOGGER = Logger.getLogger(MySQLHandler.class);
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String CONNECT_URL = "jdbc:mysql://shoprdevdb.c3qsazu8diam.us-east-1.rds.amazonaws.com:3306/shopr";
    private static final String USERNAME = "shopradmin";
    private static final String PASSWORD = "shopradmin";

    private Connection connection;
    private Properties properties;

    private Properties getProperties()
    {
        if (properties == null)
        {
            properties = new Properties();
            properties.setProperty("user", PropertiesLoader.getInstance().getProperty("mysql.username"));
            properties.setProperty("password", PropertiesLoader.getInstance().getProperty("mysql.password"));
        }
        return properties;
    }

    private Connection establishConnection()
    {
        if (connection == null)
        {
            try
            {
                connection = DriverManager.getConnection(CONNECT_URL, getProperties());
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
        return connection;
    }

    private void closeConnection()
    {
        if (connection != null)
        {
            try
            {
                connection.close();
                connection = null;
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

    public int insertFailureState(String pipelineName, int phase, Date ds)
    {
        PreparedStatement preparedStatement;
        String sql = "INSERT INTO data_pipeline_failure_state VALUES(?, ?, ?)";
        int status = 0;

        try
        {
            preparedStatement = establishConnection().prepareStatement(sql);
            preparedStatement.setString(1, pipelineName);
            preparedStatement.setInt(2, phase);
            preparedStatement.setDate(3, ds);
            status = preparedStatement.executeUpdate();
        }
        catch (SQLException e)
        {
            LOGGER.warn("Conflict on state failure insertion", e);
        }
        finally
        {
            closeConnection();
        }

        return status;
    }

    public int insertBestBuyProduct(BestBuyProduct product)
    {
        PreparedStatement priceTableStmt;
        String sql = "INSERT INTO table VALUES(?, ?)";
        int status = 0;

        try
        {
            priceTableStmt = establishConnection().prepareStatement(sql);
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            closeConnection();
        }

        return status;
    }
}
