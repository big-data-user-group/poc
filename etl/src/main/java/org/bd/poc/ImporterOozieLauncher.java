package org.bd.poc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.hadoop.MapReduceMain;

public class ImporterOozieLauncher extends MapReduceMain {

    private static final Log LOG = LogFactory.getLog(ImporterOozieLauncher.class);

    public static void main(String[] args) throws Exception {
        run(ImporterOozieLauncher.class, args);
    }

    @Override
    protected RunningJob submitJob(Configuration actionConf) throws Exception {
//        // override conf
        new ImporterNewAPI().initializeConfiguration(actionConf);

        return super.submitJob(actionConf);
    }
}
