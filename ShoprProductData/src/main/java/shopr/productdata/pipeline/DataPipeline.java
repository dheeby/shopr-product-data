package shopr.productdata.pipeline;

import org.apache.log4j.Logger;
import shopr.productdata.objects.Phase;
import shopr.productdata.objects.PipelineName;
import shopr.productdata.utils.EmailHandler;
import shopr.productdata.utils.LocalFileSystemHandler;
import shopr.productdata.utils.PropertiesLoader;
import shopr.productdata.utils.Utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Neil on 10/5/2016.
 *
 * @author Neil Allison
 */
public abstract class DataPipeline
{
    public final PipelineName pipelineName;
    public Logger LOGGER;

    public final String baseDir;
    public final String unzippedDir;
    public final String cleanedDir;

    public DataPipeline(PipelineName pipelineName)
    {
        this.pipelineName = pipelineName;
        LOGGER = Logger.getLogger(this.getClass());
        baseDir = PropertiesLoader.getInstance().getProperty("dir.tmp.dst." + pipelineName.name());
        unzippedDir = baseDir + File.separator + "unzipped";
        cleanedDir = baseDir + File.separator + "cleaned";
    }

    public boolean executeDataPipeline(Phase phase)
    {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Executing BestBuy data pipeline...");

        switch (phase)
        {
            case ALL:
                // Delete directory in case it is still there from previous failed execution
                if (Files.exists(Paths.get(baseDir)))
                {
                    LocalFileSystemHandler.deleteDirectory(baseDir);
                }
                if (Files.notExists(Paths.get(baseDir)))
                {
                    LocalFileSystemHandler.createDirectory(baseDir);
                }
            case DATARETRIEVAL:
                if (!executeDataRetrievalPhase(baseDir))
                {
                    EmailHandler.sendFailureEmail(pipelineName, Phase.DATARETRIEVAL.name());
                    Utils.insertFailureState(pipelineName, Phase.DATARETRIEVAL.name());
                    return false;
                }
            case PREPROCESS:
                if (!executePreProcessPhase(baseDir))
                {
                    EmailHandler.sendFailureEmail(pipelineName, Phase.PREPROCESS.name());
                    Utils.insertFailureState(pipelineName, Phase.PREPROCESS.name());
                    return false;
                }
            case SANITIZATION:
                if (!executeSanitizationPhase(unzippedDir))
                {
                    EmailHandler.sendFailureEmail(pipelineName, Phase.SANITIZATION.name());
                    Utils.insertFailureState(pipelineName, Phase.SANITIZATION.name());
                    return false;
                }
            case S3UPLOAD:
                if (!executeS3UploadPhase(cleanedDir))
                {
                    EmailHandler.sendFailureEmail(pipelineName, Phase.S3UPLOAD.name());
                    Utils.insertFailureState(pipelineName, Phase.S3UPLOAD.name());
                    return false;
                }
            case DBINSERTION:
                if (!executeDbInsertionPhase(cleanedDir))
                {
                    EmailHandler.sendFailureEmail(pipelineName, Phase.DBINSERTION.name());
                    Utils.insertFailureState(pipelineName, Phase.DBINSERTION.name());
                    return false;
                }
                break;
            case NONE:
                LOGGER.info(String.format("NONE command line option received. Skipping %s data pipeline.", pipelineName.name()));
                return true;
        }
        LocalFileSystemHandler.deleteDirectory(baseDir);
        Utils.cleanupFailureStateTable(pipelineName);

        long elapsedTime = System.currentTimeMillis() - startTime;
        EmailHandler.sendSuccessEmail(pipelineName, Utils.formatTime(elapsedTime));
        LOGGER.info("BestBuy data pipeline complete");
        return true;
    }

    protected abstract boolean executeDataRetrievalPhase(String dataDir);
    protected abstract boolean executePreProcessPhase(String dataDir);
    protected abstract boolean executeSanitizationPhase(String dataDir);
    protected abstract boolean executeS3UploadPhase(String dataDir);
    protected abstract boolean executeDbInsertionPhase(String dataDir);
}
