package shopr.productdata.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by Neil on 9/27/2016.
 */
public class S3Handler
{
    private static final Logger LOGGER = Logger.getLogger(S3Handler.class);

    public static boolean uploadToS3(String bucket, String key, File file)
    {
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try
        {
            s3Client.putObject(bucket, key, file);
        }
        catch (AmazonServiceException e)
        {
            LOGGER.error("An error occurred making the upload request or handling response", e);
            return false;
        }
        catch (AmazonClientException e)
        {
            LOGGER.error("An error occurred processing the upload request", e);
            return false;
        }
        return true;
    }
}
