package berlin.tu.csb.controller;

import berlin.tu.csb.model.WorkloadQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AnalyzeController {

    public static void main(String[] args) {
        List<String> outputStrings = new ArrayList<>();

        String workloadDirectorySubDirName = "6suts_4cpus_4t";


        Map<String, ArrayList<WorkloadQuery>> workloadQueryMap = getWorkloadQueriesByThread(workloadDirectorySubDirName);

        int queryCounter = workloadQueryMap.values().stream().mapToInt(List::size).sum();


        Map<String, List<Double>> mapOfLatenciesPerQueryType = getLatenciesPerQueryType(workloadQueryMap.keySet().stream().map(workloadQueryMap::get).flatMap(List::stream).collect(Collectors.toList()));

        DescriptiveStatistics selectDS = new DescriptiveStatistics();
        mapOfLatenciesPerQueryType.get("SELECT").forEach(selectDS::addValue);

        DescriptiveStatistics joinDS = new DescriptiveStatistics();
        mapOfLatenciesPerQueryType.get("JOIN").forEach(joinDS::addValue);

        DescriptiveStatistics insertDS = new DescriptiveStatistics();
        mapOfLatenciesPerQueryType.get("INSERT").forEach(insertDS::addValue);

        DescriptiveStatistics updateDS = new DescriptiveStatistics();
        mapOfLatenciesPerQueryType.get("UPDATE").forEach(updateDS::addValue);

        String selectStatisticsString = String.format("Statistics for SELECT Statements - Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f, Percent of complete run: %.2f%%\n", selectDS.getMin(), selectDS.getMax(), selectDS.getMean(), selectDS.getPercentile(25), selectDS.getPercentile(50), selectDS.getPercentile(75), selectDS.getPercentile(90), selectDS.getPercentile(95), selectDS.getPercentile(99), ((double) selectDS.getN() / queryCounter) * 100);
        String joinStatisticsString = String.format("Statistics for JOIN Statements - Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f, Percent of complete run: %.2f%%\n", joinDS.getMin(), joinDS.getMax(), joinDS.getMean(), joinDS.getPercentile(25), joinDS.getPercentile(50), joinDS.getPercentile(75), joinDS.getPercentile(90), joinDS.getPercentile(95), joinDS.getPercentile(99), ((double) joinDS.getN() / queryCounter) * 100);
        String insertStatisticsString = String.format("Statistics for INSERT Statements - Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f, Percent of complete run: %.2f%%\n", insertDS.getMin(), insertDS.getMax(), insertDS.getMean(), insertDS.getPercentile(25), insertDS.getPercentile(50), insertDS.getPercentile(75), insertDS.getPercentile(90), insertDS.getPercentile(95), insertDS.getPercentile(99), ((double) insertDS.getN() / queryCounter) * 100);
        String updateStatisticsString = String.format("Statistics for UPDATE Statements - Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f, Percent of complete run: %.2f%%\n", updateDS.getMin(), updateDS.getMax(), updateDS.getMean(), updateDS.getPercentile(25), updateDS.getPercentile(50), updateDS.getPercentile(75), updateDS.getPercentile(90), updateDS.getPercentile(95), updateDS.getPercentile(99), ((double) updateDS.getN() / queryCounter) * 100);
        System.out.print(selectStatisticsString);
        System.out.print(joinStatisticsString);
        System.out.print(insertStatisticsString);
        System.out.print(updateStatisticsString);

        outputStrings.add(selectStatisticsString);
        outputStrings.add(joinStatisticsString);
        outputStrings.add(insertStatisticsString);
        outputStrings.add(updateStatisticsString);


        Map<String, ArrayList<WorkloadQuery>> workloadQueryByVMMap = getWorkloadQueriesByVM(workloadDirectorySubDirName);

        List<String> latencyByVMs = new ArrayList<>();
        List<String> latencyByVMsRawValues = new ArrayList<>();
        workloadQueryByVMMap.forEach((s, workloadQueries) -> {
            DescriptiveStatistics descriptiveStatisticsByVM = new DescriptiveStatistics();
            workloadQueries.forEach(workloadQuery -> {
                double rtt = getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit);
                descriptiveStatisticsByVM.addValue(rtt);
            });
            String latencyByVMString = String.format("Values for vm with id %s for %d entries. Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f\n", s, descriptiveStatisticsByVM.getN(), descriptiveStatisticsByVM.getMin(), descriptiveStatisticsByVM.getMax(), descriptiveStatisticsByVM.getMean(), descriptiveStatisticsByVM.getPercentile(25), descriptiveStatisticsByVM.getPercentile(50), descriptiveStatisticsByVM.getPercentile(75), descriptiveStatisticsByVM.getPercentile(90), descriptiveStatisticsByVM.getPercentile(95), descriptiveStatisticsByVM.getPercentile(99));
            latencyByVMs.add(latencyByVMString);
            System.out.print(latencyByVMString);
            latencyByVMsRawValues.add(Arrays.toString(descriptiveStatisticsByVM.getValues()));
        });

        outputStrings.addAll(latencyByVMs);




        //System.exit(0);

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
            //System.out.println(Arrays.toString(descriptiveStatistics.getValues()));
            System.out.printf("Calculating the Values for %s with %d total entries. Min value: %f, Max value: %f, Average Value: %f\n", key, descriptiveStatistics.getN(), descriptiveStatistics.getMin(), descriptiveStatistics.getMax(), descriptiveStatistics.getMean());

        });
        //System.out.println(Arrays.toString(descriptiveStatisticsAll.getValues()));
        String rttStatisticsForAllString = String.format("Calculating rtt for all %d entries: Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f\n", descriptiveStatisticsAll.getN(), descriptiveStatisticsAll.getMin(), descriptiveStatisticsAll.getMax(), descriptiveStatisticsAll.getMean(), descriptiveStatisticsAll.getPercentile(25), descriptiveStatisticsAll.getPercentile(50), descriptiveStatisticsAll.getPercentile(75), descriptiveStatisticsAll.getPercentile(90), descriptiveStatisticsAll.getPercentile(95), descriptiveStatisticsAll.getPercentile(99));
        System.out.print(rttStatisticsForAllString);
        outputStrings.add(rttStatisticsForAllString);

        List<WorkloadQuery> listOfAllWorkloadQueries = new ArrayList<>();
        workloadQueryMap.forEach((s, workloadQueryList) -> listOfAllWorkloadQueries.addAll(workloadQueryList));
        Map<Long, List<Long>> mapOfPingsPerSecond = createListWithSummedQueriesPerSecond(listOfAllWorkloadQueries);
        Pair<Long, Long> minMaxPair = getMinMaxTimeMillis(listOfAllWorkloadQueries);
        mapOfPingsPerSecond = fillGaps(mapOfPingsPerSecond, minMaxPair.getKey(), minMaxPair.getValue());

        Map<Long, Long> mapOfRTTPerSecondWithAverages = new LinkedHashMap<>();
        Map<Long, Integer> mapOfTransactionsPerSecond = new LinkedHashMap<>();
        DescriptiveStatistics rollingDS = new DescriptiveStatistics();
        DescriptiveStatistics qpsDS = new DescriptiveStatistics();

        AtomicInteger counter = new AtomicInteger();
        mapOfPingsPerSecond.forEach((aLong, longs) -> {
            // cut of warmup and cooldown
            Instant currentTime = Instant.ofEpochMilli(aLong);
            long cutOffSecondsBegin = 120;
            long cutOffSecondsEnd = 20;
            if(currentTime.isAfter(Instant.ofEpochMilli(minMaxPair.getKey()).plusSeconds(cutOffSecondsBegin)) && currentTime.isBefore(Instant.ofEpochMilli(minMaxPair.getValue()).minusSeconds(cutOffSecondsEnd))) {
                mapOfRTTPerSecondWithAverages.put(aLong, longs.stream().mapToLong(Long::longValue).sum() / longs.size());
                rollingDS.addValue(longs.stream().mapToLong(Long::longValue).sum() / longs.size());

                mapOfTransactionsPerSecond.put(aLong, longs.size());
                qpsDS.addValue(longs.size());

            }
            counter.getAndIncrement();
        });

        //System.out.println(Arrays.toString(rollingDS.getValues()));
        String latencyPerSecondStatistic = String.format("Important values for normalized time series Latency. count: %d, Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f\n", rollingDS.getN(), rollingDS.getMin(), rollingDS.getMax(), rollingDS.getMean(), rollingDS.getPercentile(25), rollingDS.getPercentile(50), rollingDS.getPercentile(75), rollingDS.getPercentile(90), rollingDS.getPercentile(95), rollingDS.getPercentile(99));
        //System.out.println(Arrays.toString(qpsDS.getValues()));
        String transactionsPerSecondStatistics = String.format("Important values for normalized time series Transactions per Second. Count: %d, Min value: %f, Max value: %f, Average Value: %f, 25th percentile:%f, 50th percentile:%f, 75th percentile:%f, 90th percentile:%f, 95th percentile:%f, 99th percentile:%f\n", qpsDS.getN(), qpsDS.getMin(), qpsDS.getMax(), qpsDS.getMean(), qpsDS.getPercentile(25), qpsDS.getPercentile(50), qpsDS.getPercentile(75), qpsDS.getPercentile(90), qpsDS.getPercentile(95), qpsDS.getPercentile(99));

        System.out.print(latencyPerSecondStatistic);
        System.out.print(transactionsPerSecondStatistics);

        outputStrings.add(latencyPerSecondStatistic);
        outputStrings.add(transactionsPerSecondStatistics);

        outputStrings.add(String.format("Raw values:%n%n"));
        int id = 1;
        for (Object s: latencyByVMsRawValues.toArray()) {
            outputStrings.add(String.format("Raw Values latency per VM ID - %d:%n%n%s%n%n%n%n***", id++, (String) s));
        }

        outputStrings.add(String.format("Raw Values latency unsorted for select statements:%n%n%s%n%n%n***", Arrays.toString(selectDS.getValues())));
        outputStrings.add(String.format("Raw Values latency unsorted for join statements:%n%n%s%n%n%n***", Arrays.toString(joinDS.getValues())));
        outputStrings.add(String.format("Raw Values latency unsorted for insert statements:%n%n%s%n%n%n***", Arrays.toString(insertDS.getValues())));
        outputStrings.add(String.format("Raw Values latency unsorted for update statements:%n%n%s%n%n%n***", Arrays.toString(updateDS.getValues())));

        outputStrings.add(String.format("Raw Values latency unsorted for complete run:%n%n%s%n%n%n***", Arrays.toString(descriptiveStatisticsAll.getValues())));
        outputStrings.add(String.format("1 sec latency for complete run:%n%n%s%n%n%n***", Arrays.toString(rollingDS.getValues())));
        outputStrings.add(String.format("1 sec transactions per second for complete run:%n%n%s%n%n%n***", Arrays.toString(qpsDS.getValues())));


        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName, "statistics.txt");
        try(BufferedWriter writer = Files.newBufferedWriter(filePath, Charset.forName("UTF-8"))) {
            for (String s: outputStrings) {
                writer.write(s + System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

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

    public static Map<String, ArrayList<WorkloadQuery>> getWorkloadQueriesByThread(String workloadDirectorySubDirName) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName);
        Path filePathCache = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName, workloadDirectorySubDirName + "_cache.json");

        if(Files.exists(filePathCache)) {
            return loadWorkloadQueriesCache(filePathCache);
        }

        File folder = new File(filePath.toUri());
        File[] files = folder.listFiles();

        int fileCounter = 0;
        int workloadQueryCounter = 0;
        Map<String, ArrayList<WorkloadQuery>> workloadQueryMap = new HashMap<>();
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
                String key = "Thread-" + workloadQueriesFromFile[0].workloadContextId;
                if (workloadQueryMap.get(key) == null) {
                    workloadQueryMap.put(key, new ArrayList<WorkloadQuery>(Arrays.asList(workloadQueriesFromFile)));
                }
                else {
                    workloadQueryMap.get(key).addAll(Arrays.asList(workloadQueriesFromFile));
                }

                workloadQueryCounter += workloadQueriesFromFile.length;
            }
        }

        saveWorkloadQueriesCache(workloadQueryMap, workloadDirectorySubDirName);

        System.out.printf("Read %d workloadQueries from %d files of the run at %s\n", workloadQueryCounter, fileCounter, workloadDirectorySubDirName);
        return workloadQueryMap;
    }

    public static Map<String, ArrayList<WorkloadQuery>> getWorkloadQueriesByVM(String workloadDirectorySubDirName) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName);
        Path filePathCache = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName, workloadDirectorySubDirName + "_vm_cache.json");

        if(Files.exists(filePathCache)) {
            return loadWorkloadQueriesCache(filePathCache);
        }

        File folder = new File(filePath.toUri());
        File[] files = folder.listFiles();

        int fileCounter = 0;
        int workloadQueryCounter = 0;
        Map<String, ArrayList<WorkloadQuery>> workloadQueryMap = new HashMap<>();
        for (File file : files) {
            // is file in format 'run_16 (2).json' where '(2)' is meant to be the vm id
            if (file.isFile() && file.getName().contains("run_") && file.getName().contains("(") && file.getName().contains(")")) {
                String readedJson = null;
                try {
                    readedJson = Files.readString(file.toPath());
                    fileCounter++;
                } catch (
                        IOException e) {
                    e.printStackTrace();
                }
                WorkloadQuery[] workloadQueriesFromFile = gson.fromJson(readedJson, WorkloadQuery[].class);
                String key = "vm-" + StringUtils.substringBetween(file.getName(), "(", ")");;
                if (workloadQueryMap.get(key) == null) {
                    workloadQueryMap.put(key, new ArrayList<WorkloadQuery>(Arrays.asList(workloadQueriesFromFile)));
                }
                else {
                    workloadQueryMap.get(key).addAll(Arrays.asList(workloadQueriesFromFile));
                }

                workloadQueryCounter += workloadQueriesFromFile.length;
            }
            // is file in format 'run_16.json' which is vm id 1
            else if (file.isFile() && file.getName().contains("run_") && !file.getName().contains("(") && !file.getName().contains(")")) {
                String readedJson = null;
                try {
                    readedJson = Files.readString(file.toPath());
                    fileCounter++;
                } catch (
                        IOException e) {
                    e.printStackTrace();
                }
                WorkloadQuery[] workloadQueriesFromFile = gson.fromJson(readedJson, WorkloadQuery[].class);
                String key = "vm-" + 1;
                if (workloadQueryMap.get(key) == null) {
                    workloadQueryMap.put(key, new ArrayList<WorkloadQuery>(Arrays.asList(workloadQueriesFromFile)));
                }
                else {
                    workloadQueryMap.get(key).addAll(Arrays.asList(workloadQueriesFromFile));
                }

                workloadQueryCounter += workloadQueriesFromFile.length;
            }
        }


        saveWorkloadQueriesByVMCache(workloadQueryMap, workloadDirectorySubDirName);

        System.out.printf("Read %d workloadQueries from %d files of the run at %s\n", workloadQueryCounter, fileCounter, workloadDirectorySubDirName);
        return workloadQueryMap;
    }

    private static void saveWorkloadQueriesCache(Map<String, ArrayList<WorkloadQuery>> workloadQueryMap, String workloadDirectorySubDirName) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String json = gson.toJson(workloadQueryMap);
        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName, workloadDirectorySubDirName + "_cache.json");

        try {
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, json, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveWorkloadQueriesByVMCache(Map<String, ArrayList<WorkloadQuery>> workloadQueryMap, String workloadDirectorySubDirName) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String json = gson.toJson(workloadQueryMap);
        Path filePath = Paths.get(System.getProperty("user.dir"), "workload", workloadDirectorySubDirName, workloadDirectorySubDirName + "_vm_cache.json");

        try {
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, json, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, ArrayList<WorkloadQuery>> loadWorkloadQueriesCache(Path pathToCacheFile) {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        Map<String, ArrayList<WorkloadQuery>> workloadQueryMap = null;

        String readedJson = null;
        try {
            readedJson = Files.readString(pathToCacheFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Type type = new TypeToken<Map<String, ArrayList<WorkloadQuery>>>(){}.getType();
        workloadQueryMap = gson.fromJson(readedJson, type);

        System.out.println("loaded cached file " + pathToCacheFile.getFileName().toString());
        return workloadQueryMap;
    }

    public static Map<String, List<Double>> getLatenciesPerQueryType(List<WorkloadQuery> workloadQueryList) {
        Map<String, List<Double>> mapOfLatencyPerQueryType = new HashMap<>();
        mapOfLatencyPerQueryType.put("SELECT", workloadQueryList.stream()
                .filter(workloadQuery -> workloadQuery.sqlString.contains("SELECT"))
                .filter(workloadQuery -> !workloadQuery.sqlString.contains("JOIN"))
                .map(workloadQuery -> getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit))
                .collect(Collectors.toList()));
        mapOfLatencyPerQueryType.put("JOIN", workloadQueryList.stream()
                .filter(workloadQuery -> workloadQuery.sqlString.contains("JOIN"))
                .map(workloadQuery -> getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit))
                .collect(Collectors.toList()));
        mapOfLatencyPerQueryType.put("INSERT", workloadQueryList.stream()
                .filter(workloadQuery -> workloadQuery.sqlString.contains("INSERT"))
                .map(workloadQuery -> getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit))
                .collect(Collectors.toList()));
        mapOfLatencyPerQueryType.put("UPDATE", workloadQueryList.stream()
                .filter(workloadQuery -> workloadQuery.sqlString.contains("UPDATE"))
                .map(workloadQuery -> getDifferenceOfTimestampsInMilliseconds(workloadQuery.timestampBeforeCommit, workloadQuery.timestampAfterCommit))
                .collect(Collectors.toList()));
        return mapOfLatencyPerQueryType;
    }


}
