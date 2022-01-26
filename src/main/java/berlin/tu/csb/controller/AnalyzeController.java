package berlin.tu.csb.controller;

import berlin.tu.csb.model.WorkloadQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalyzeController {

    public static void main(String[] args) {
        String workloadDirectorySubDirName = "2022-01-25 15.49.53.443";

        Map<String, List<WorkloadQuery>> workloadQueryMap = getWorkloadQueries(workloadDirectorySubDirName);

        DescriptiveStatistics descriptiveStatisticsAll = new DescriptiveStatistics();
        workloadQueryMap.forEach((key, value) -> {
            DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
            value.forEach((workloadQuery -> {
            if (!workloadQuery.timestampBeforeCommit.isBlank()) { // TODO: remove when fixed in bulk insert orderlines

                double rtt = getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit);
                descriptiveStatistics.addValue(rtt);
                descriptiveStatisticsAll.addValue(rtt);
            }
            }));
            System.out.println(Arrays.toString(descriptiveStatistics.getValues()));
            System.out.printf("Calculating the Values for %s with %d total entries. Min value: %f, Max value: %f, Average Value: %f\n", key, descriptiveStatistics.getN(), descriptiveStatistics.getMin(), descriptiveStatistics.getMax(), descriptiveStatistics.getMean());

        });
        System.out.println(Arrays.toString(descriptiveStatisticsAll.getValues()));
        System.out.printf("Values for all raw sets with %d total entries. Min value: %f, Max value: %f, Average Value: %f\n", descriptiveStatisticsAll.getN(), descriptiveStatisticsAll.getMin(), descriptiveStatisticsAll.getMax(), descriptiveStatisticsAll.getMean());


        List<WorkloadQuery> listOfAllWorkloadQueries = new ArrayList<>();
        workloadQueryMap.forEach((s, workloadQueryList) -> listOfAllWorkloadQueries.addAll(workloadQueryList));
        Map<Long, List<Long>> mapOfPingsPerSecond = createListWithSummedQueriesPerSecond(listOfAllWorkloadQueries);
        Pair<Long, Long> minMaxPair = getMinMaxTimeMillis(listOfAllWorkloadQueries);
        mapOfPingsPerSecond = fillGaps(mapOfPingsPerSecond, minMaxPair.getKey(), minMaxPair.getValue());

        Map<Long, Long> mapOfRTTPerSecondWithAverages = new LinkedHashMap<>();
        DescriptiveStatistics rollingDS = new DescriptiveStatistics();

        AtomicInteger counter = new AtomicInteger();
        mapOfPingsPerSecond.forEach((aLong, longs) -> {
            // cut of warmup and cooldown
            Instant currentTime = Instant.ofEpochMilli(aLong);
            if(currentTime.isAfter(Instant.ofEpochMilli(minMaxPair.getKey()).plusSeconds(10)) && currentTime.isBefore(Instant.ofEpochMilli(minMaxPair.getValue()).minusSeconds(10))) {
                mapOfRTTPerSecondWithAverages.put(aLong, longs.stream().mapToLong(Long::longValue).sum() / longs.size());
                rollingDS.addValue(longs.stream().mapToLong(Long::longValue).sum() / longs.size());
            }
            counter.getAndIncrement();
        });

        System.out.println(Arrays.toString(rollingDS.getValues()));
        System.out.printf("Important values for normalized time series. min:%f max:%f average:%f 99th percentile:%f\n", rollingDS.getMin(), rollingDS.getMax(), rollingDS.getMean(), rollingDS.getPercentile(99));



    }



    public static Map<Long, List<Long>> fillGaps(Map<Long, List<Long>> mapOfPingsPerSecond, long startMillis, long endMillis) {
        Instant i = Instant.ofEpochMilli(startMillis).truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        for (;i.isBefore(Instant.ofEpochMilli(endMillis)); i = i.plusSeconds(1)) {
            if (mapOfPingsPerSecond.get(i.toEpochMilli()) == null) {
                mapOfPingsPerSecond.put(i.toEpochMilli(), new ArrayList<>());

                // Look for next an previos value to build average over them. Use only previous when there are bigger gaps
                Long previousValue = mapOfPingsPerSecond.get(i.minusSeconds(1).toEpochMilli()).get(0);
                Long nextValue;
                if (mapOfPingsPerSecond.get(i.plusSeconds(1).toEpochMilli()) == null) {
                    nextValue = previousValue;
                }
                else {
                    nextValue = mapOfPingsPerSecond.get(i.plusSeconds(1).toEpochMilli()).get(0);
                }
                mapOfPingsPerSecond.get(i.toEpochMilli()).add((previousValue + nextValue) / 2);

                System.out.printf("Filled gap at %d with average of %d and %d\n", i.getEpochSecond(), previousValue, nextValue);
            }
        }
        return mapOfPingsPerSecond;
    }

    public static Pair<Long, Long> getMinMaxTimeMillis(List<WorkloadQuery> workloadQueryList) {
        DescriptiveStatistics ds = new DescriptiveStatistics();

        workloadQueryList.forEach(workloadQuery -> {

            if (!workloadQuery.timestampAfterCommit.isBlank() && !workloadQuery.timestampBeforeCommit.isBlank()) {


                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");

                Instant before = null;
                Instant after = null;
                try {
                    before = format.parse(workloadQuery.timestampBeforeCommit).toInstant();
                    after = format.parse(workloadQuery.timestampAfterCommit).toInstant();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                ds.addValue(before.toEpochMilli());
                ds.addValue(after.toEpochMilli());
            }
        });

        return new Pair<Long, Long>((long) ds.getMin(), (long) ds.getMax());

    }

    public static Map<Long, List<Long>> createListWithSummedQueriesPerSecond(List<WorkloadQuery> workloadQueryList) {
        DescriptiveStatistics dsStart = new DescriptiveStatistics();
        DescriptiveStatistics dsEnd = new DescriptiveStatistics();
        Map<Long, List<Long>> mapOfPingsPerSecond = new LinkedHashMap<>();
        workloadQueryList.forEach(workloadQuery -> {

            if (!workloadQuery.timestampAfterCommit.isBlank() && !workloadQuery.timestampBeforeCommit.isBlank()) {


                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");

                Instant before = null;
                Instant after = null;
                try {
                    before = format.parse(workloadQuery.timestampBeforeCommit).toInstant();
                    after = format.parse(workloadQuery.timestampAfterCommit).toInstant();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                dsStart.addValue(before.toEpochMilli());
                dsEnd.addValue(after.toEpochMilli());

                Instant afterWithoutMilliseconds = after.truncatedTo(ChronoUnit.SECONDS);

                if (mapOfPingsPerSecond.get(afterWithoutMilliseconds.toEpochMilli()) == null) {
                    mapOfPingsPerSecond.put(afterWithoutMilliseconds.toEpochMilli(), new ArrayList<>());
                }
                mapOfPingsPerSecond.get(afterWithoutMilliseconds.toEpochMilli()).add(Duration.between(before, after).toMillis());
            }
        });

        System.out.printf("Duration of experiment in seconds: %d and number of lists in map: %d \n", Duration.between(Instant.ofEpochMilli((long) dsStart.getMin()), Instant.ofEpochMilli((long) dsEnd.getMax())).toSeconds(), mapOfPingsPerSecond.size());

        System.out.printf("First event at %f and last event at %f \n", dsStart.getMin(), dsEnd.getMax());
        return mapOfPingsPerSecond;
    }

    public static double getDifferenceOfTimestampsInMilliseconds(String timestampFirst, String timestampLast) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");

        Date first = null;
        Date second = null;
        try {
            first = format.parse(timestampFirst);
            second = format.parse(timestampLast);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return second.getTime() - first.getTime();

    }

    public static Map<String, List<WorkloadQuery>> getWorkloadQueries(String workloadDirectorySubDirName) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName);
        File folder = new File(filePath.toUri());
        File[] files = folder.listFiles();

        int fileCounter = 0;
        int workloadQueryCounter = 0;
        Map<String, List<WorkloadQuery>> workloadQueryMap = new HashMap<>();
        for (File file : files) {
            if (file.isFile() && file.getName().contains("run_")) {
                String readedJson = null;
                try {
                    readedJson = Files.readString(file.toPath());
                    fileCounter++;
                } catch (
                        IOException e) {
                    e.printStackTrace();
                }
                WorkloadQuery[] workloadQueriesFromFile = gson.fromJson(readedJson, WorkloadQuery[].class);
                workloadQueryMap.put("Thread-" + workloadQueriesFromFile[0].workloadContextId, Arrays.asList(workloadQueriesFromFile));
                workloadQueryCounter += workloadQueriesFromFile.length;
            }
        }

        System.out.printf("Read %d workloadQueries from %d files of the run at %s\n", workloadQueryCounter, fileCounter, workloadDirectorySubDirName);
        return workloadQueryMap;
    }


}
