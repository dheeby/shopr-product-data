package shopr.productdata.objects;

/**
 * Created by Neil on 9/28/2016.
 *
 * @author Neil Allison
 */
public enum Phase
{
    ALL_PHASES,
    DATA_RETRIEVAL_PHASE,
    DATA_PRE_PROCESS_PHASE,
    DATA_SANITIZATION_PHASE,
    DATA_S3_UPLOAD_PHASE,
    DATA_DB_INSERTION_PHASE,
    NONE
}
