package shopr.productdata.utils;

import org.apache.log4j.Logger;
import shopr.productdata.objects.PipelineName;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Created by Neil on 9/25/2016.
 *
 * @author Neil Allison
 */
public class EmailHandler
{
    private static final Logger LOGGER = Logger.getLogger(EmailHandler.class);

    public static void sendSuccessEmail(PipelineName pipelineName, String formattedCompletionTime)
    {
        String subject = "[ShoprProductDataPipeline] Data Pipeline Success";
        String body = String.format("Data pipeline success.%nPipeline: %s%nExecution Time: %s",
                pipelineName.name(), formattedCompletionTime);
        sendEmail(subject, body);
    }

    public static void sendFailureEmail(PipelineName pipelineName, String phase)
    {
        String subject = "[ShoprProductDataPipeline] Data Pipeline Failure";
        String body = String.format("Failure in data pipeline.%nPipeline: %s%nPhase: %s", pipelineName.name(), phase);
        sendEmail(subject, body);
    }

    private static void sendEmail(String subject, String body)
    {
        Properties properties = System.getProperties();
        properties.put("mail.smtp.port", "587");
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.ssl.trust", "smtp.gmail.com");

        Session session = Session.getDefaultInstance(properties, null);
        MimeMessage message = new MimeMessage(session);
        try
        {
            String recipients = PropertiesLoader.getInstance().getProperty("failure.email.list");
            message.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
            message.setSubject(subject);
            message.setText(body);

            Transport transport = session.getTransport("smtp");
            transport.connect("smtp.gmail.com", PropertiesLoader.getInstance().getProperty("shopr.gmail.username"),
                    PropertiesLoader.getInstance().getProperty("shopr.gmail.password"));
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();
        }
        catch (MessagingException e)
        {
            LOGGER.error("Sending email failed", e);
        }
    }
}
