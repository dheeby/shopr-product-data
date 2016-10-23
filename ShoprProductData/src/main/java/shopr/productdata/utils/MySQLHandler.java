package shopr.productdata.utils;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by Neil on 9/27/2016.
 *
 * @author Neil Allison
 */
public class MySQLHandler
{
    private static final Logger LOGGER = Logger.getLogger(MySQLHandler.class);
    private static final String CONNECT_URL = "jdbc:mysql://shoprdevdb.c3qsazu8diam.us-east-1.rds.amazonaws.com:3306/shopr";

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

    public int insertFailureState(String pipelineName, String phase, Date ds)
    {
        PreparedStatement preparedStatement;
        String sql = "INSERT INTO data_pipeline_failure_state (pipelineName, phase, ds) VALUES(?, ?, ?)";
        int status = 0;

        try
        {
            preparedStatement = establishConnection().prepareStatement(sql);
            preparedStatement.setString(1, pipelineName);
            preparedStatement.setString(2, phase);
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

    public boolean loadDataLocalInfileCsv(String filename, String table)
    {
        Statement stmt;
        String sql = String.format(
                "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY \'\\0\' LINES TERMINATED BY \'\\n\'",
                filename, table
        );
        boolean status = true;
        try
        {
            stmt = establishConnection().createStatement();
            stmt.execute(sql);
        }
        catch (SQLException e)
        {
            LOGGER.warn("MySQL data load failed for file: " + filename, e);
            status = false;
        }
        finally
        {
            closeConnection();
        }

        return status;
    }

    public void deleteFailureStates(String pipelineName)
    {
        PreparedStatement stmt;
        String sql = "DELETE FROM data_pipeline_failure_state WHERE pipelineName=?";
        try
        {
            stmt = establishConnection().prepareStatement(sql);
            stmt.setString(1, pipelineName);

            stmt.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public ArrayList<String[]> getPipelineFailureStates()
    {
        ArrayList<String[]> failureStates = new ArrayList<>();
        Statement stmt;
        String sql = "SELECT * FROM data_pipeline_failure_state";

        try
        {
            stmt = establishConnection().createStatement();
            stmt.executeQuery(sql);
            ResultSet rs = stmt.getResultSet();
            while (rs.next())
            {
                String[] failureState = new String[3];
                failureState[0] = rs.getString(1);
                failureState[1] = rs.getString(2);
                failureState[2] = rs.getDate(3).toString();
                failureStates.add(failureState);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            closeConnection();
        }

        return failureStates;
    }
}
