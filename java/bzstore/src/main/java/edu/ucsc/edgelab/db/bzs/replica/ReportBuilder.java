package edu.ucsc.edgelab.db.bzs.replica;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReportBuilder {

    private final FileWriter writer;
    private static final Logger LOGGER = Logger.getLogger(ReportBuilder.class.getName());

    public ReportBuilder(String[] fields) throws IOException {
        String reportFileName = System.getProperty("user.dir") + "/Report_" + ReportBuilder.getDateString() + ".csv";
        writer = new FileWriter(new File(reportFileName));

        for (String field : fields) {
            writer.write(field);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down report writer.");
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close report file writer.", e);
            }
        }));


    }

    public void writeLine(String line) {
        try {
            writer.write(line);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Exception occurred while writing report: " + line,e);
        }
    }


    public static String getDateString() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss");
        return sdf.format(date);
    }
}
