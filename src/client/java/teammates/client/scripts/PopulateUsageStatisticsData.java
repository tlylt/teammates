package teammates.client.scripts;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.cloud.datastore.DatastoreOptions;
import com.googlecode.objectify.ObjectifyFactory;
import com.googlecode.objectify.ObjectifyService;

import teammates.client.util.ClientProperties;
import teammates.common.datatransfer.attributes.UsageStatisticsAttributes;
import teammates.common.datatransfer.logs.LogEvent;
import teammates.common.util.Config;
import teammates.common.util.JsonUtils;
import teammates.common.util.TimeHelper;
import teammates.logic.api.LogsProcessor;
import teammates.logic.core.LogicStarter;
import teammates.logic.core.UsageStatisticsLogic;
import teammates.storage.api.OfyHelper;

/**
 * Populates past usage statistics data based on entities in the database and logs in the logs collection service.
 * Notably, this will not be able to capture entities already deleted or logs already discarded,
 * but this is the best attempt that we have.
 *
 * <p>The earliest timestamp is determined to be start of 2016 as more than 90% of the application data
 * was created after this time.
 *
 * <p>Note: If logs-related statistics is collected, one needs to modify {@link LogsProcessor} constructor
 * such that the service used is always the production Cloud Logging service.
 */
public class PopulateUsageStatisticsData {

    // Modify this as necessary. The default date is the latest date where a stats object does not exist.
    private static final Instant START_TIMESTAMP = Instant.parse("2022-03-30T16:00:00Z");

    // Start of 2016 in UTC, plus one second so that the script knows to stop when reaching this time
    private static final Instant END_TIMESTAMP = Instant.parse("2015-12-31T16:00:01Z");

    private static final long ONE_HOUR_IN_MS = 60 * 60 * 1000L;

    private static final long THIRTY_DAYS_IN_MS = 30 * 24 * ONE_HOUR_IN_MS;

    private static final int NUM_THREADS = 168;

    private static final boolean IS_PREVIEW = true;

    private final UsageStatisticsLogic usageStatisticsLogic = UsageStatisticsLogic.inst();
    private final LogsProcessor logsProcessor = LogsProcessor.inst();

    public static void main(String[] args) {
        new PopulateUsageStatisticsData().doOperation();
    }

    private void doOperation() {
        Instant startTimestamp = TimeHelper.getInstantNearestHourBefore(START_TIMESTAMP);
        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        List<Instant> startTimes = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            startTimes.add(startTimestamp.minusMillis(i * ONE_HOUR_IN_MS));
        }

        List<CompletableFuture<?>> futures = new ArrayList<>();
        startTimes.forEach(startTime0 -> futures.add(CompletableFuture.runAsync(() -> {
            // Need to copy the Datastore client setup here, as Objectify context is not persisted to new threads
            DatastoreOptions.Builder builder = DatastoreOptions.newBuilder().setProjectId(Config.APP_ID);
            if (ClientProperties.isTargetUrlDevServer()) {
                builder.setHost(ClientProperties.TARGET_URL);
            }
            ObjectifyService.init(new ObjectifyFactory(builder.build().getService()));
            OfyHelper.registerEntityClasses();
            ObjectifyService.begin();
            LogicStarter.initializeDependencies();

            Instant currentTime = startTime0;
            while (true) {
                Instant endTime = currentTime;
                Instant startTime = endTime.minusMillis(ONE_HOUR_IN_MS);

                List<UsageStatisticsAttributes> existingStats = usageStatisticsLogic.getUsageStatisticsForTimeRange(
                        startTime.minusMillis(100), startTime.plusMillis(100));
                if (!existingStats.isEmpty()) {
                    // Stats for this period has been previously collected; skip to next period
                    currentTime = currentTime.minusMillis(NUM_THREADS * ONE_HOUR_IN_MS);
                    if (currentTime.isBefore(END_TIMESTAMP)) {
                        break;
                    }
                    continue;
                }

                System.out.printf("Collecting statistics for time period %s - %s%n", startTime, endTime);

                try {
                    UsageStatisticsAttributes entitiesStats =
                            usageStatisticsLogic.calculateEntitiesStatisticsForTimeRange(startTime, endTime);
                    int numEmailsSent = 0;
                    int numSubmissions = 0;
                    if (Instant.now().toEpochMilli() - startTime.toEpochMilli() <= THIRTY_DAYS_IN_MS) {
                        // Logs in Cloud Logging are only kept for 30 days
                        numEmailsSent = logsProcessor.getNumberOfLogsForEvent(startTime, endTime, LogEvent.EMAIL_SENT, "");
                        numSubmissions = logsProcessor.getNumberOfLogsForEvent(startTime, endTime,
                                LogEvent.FEEDBACK_SESSION_AUDIT, "jsonPayload.accessType=\"submission\"");
                    }
                    UsageStatisticsAttributes overallUsageStats = UsageStatisticsAttributes.builder(startTime, 60)
                            .withNumResponses(entitiesStats.getNumResponses())
                            .withNumCourses(entitiesStats.getNumCourses())
                            .withNumStudents(entitiesStats.getNumStudents())
                            .withNumInstructors(entitiesStats.getNumInstructors())
                            .withNumAccountRequests(entitiesStats.getNumAccountRequests())
                            .withNumEmails(numEmailsSent)
                            .withNumSubmissions(numSubmissions)
                            .build();

                    System.out.printf("Statistics collected for %s - %s: %s%n",
                            startTime, endTime, JsonUtils.toCompactJson(overallUsageStats));
                    if (!IS_PREVIEW) {
                        usageStatisticsLogic.createUsageStatistics(overallUsageStats);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                currentTime = currentTime.minusMillis(NUM_THREADS * ONE_HOUR_IN_MS);
                if (currentTime.isBefore(END_TIMESTAMP)) {
                    break;
                }
            }
        }, es)));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {})).join();

        es.shutdown();
    }

}
