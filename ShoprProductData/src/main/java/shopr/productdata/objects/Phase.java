package shopr.productdata.objects;

/**
 * Created by Neil on 9/28/2016.
 *
 * @author Neil Allison
 */
public enum Phase
{
    ALL,
    DATARETRIEVAL,
    PREPROCESS,
    SANITIZATION,
    S3UPLOAD,
    DBINSERTION,
    NONE
}
