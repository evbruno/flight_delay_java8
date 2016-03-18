
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class Main {

    public static void main(String args[]) {

        String fileName = "/tmp/2008.csv";

        long init = System.currentTimeMillis();

        //read file into stream, try-with-resources
        try (Stream<String> sourceStream = Files.lines(Paths.get(fileName))) {

            Stream<FlightEvent> eventStream = sourceStream
                    // .limit(100000)
                    //.parallel()
                    .map(Main::parse)
                    .filter(Main::isDelayed);

            Stream<FlightDelayRecord> recordStream = eventStream
                    //.parallel()
                    .map(Main::convert);

            // terminal operation :(
            Map<String, List<FlightDelayRecord>> groupedRecords = recordStream
                    .collect(Collectors.groupingBy(FlightDelayRecord::getUniqueCarrier));

            Stream<RecordAggregate> results = groupedRecords
                    .entrySet()
                    .stream()
                    .map(
                        entry -> entry.getValue()
                            //.parallelStream()
                            .stream()
                            .map(rec -> new RecordAggregate(rec.uniqueCarrier, rec.getArrDelayMins(), 1))
                            .reduce(
                                    new RecordAggregate(entry.getKey(), 0, 0),
                                    (acc, curr) -> curr.plus(acc)
                            )

            );

            results.forEach(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();

        System.out.println("Took: " + (end - init) + " millis");

    }

    static FlightDelayRecord convert(FlightEvent evt) {
        FlightDelayRecord ret = new FlightDelayRecord();
        ret.year = evt.year;
        ret.month = evt.month;
        ret.dayOfMonth = evt.dayOfMonth;
        ret.flightNum = evt.flightNum;
        ret.uniqueCarrier = evt.uniqueCarrier;
        ret.arrDelayMins = evt.arrDelayMins;
        return ret;
    }

    static boolean isDelayed(FlightEvent evt) {
        try {
            return Integer.parseInt(evt.arrDelayMins) > 0;
        } catch (Throwable e) {
            return false;
        }
    }

    static FlightEvent parse(String line) {
        FlightEvent ret = new FlightEvent();
        String[] columns = line.split(",");
        ret.year = columns[0];
        ret.month = columns[1];
        ret.dayOfMonth = columns[2];
        ret.dayOfWeek = columns[3];
        ret.depTime = columns[4];
        ret.scheduledDepTime = columns[5];
        ret.arrTime = columns[6];
        ret.scheduledArrTime = columns[7];
        ret.uniqueCarrier = columns[8];
        ret.flightNum = columns[9];
        ret.tailNum = columns[10];
        ret.actualElapsedMins = columns[11];
        ret.crsElapsedMins = columns[12];
        ret.airMins = columns[13];
        ret.arrDelayMins = columns[14];
        ret.depDelayMins = columns[15];
        ret.originAirportCode = columns[16];
        ret.destinationAirportCode = columns[17];
        ret.distanceInMiles = columns[18];
        ret.taxiInTimeMins = columns[19];
        ret.taxiOutTimeMins = columns[20];
        ret.flightCancelled = columns[21];
        ret.cancellationCode = columns[22]; // (A = carrier, B = weather, C = NAS, D = security)
        ret.diverted = columns[23]; // 1 = yes, 0 = no
        ret.carrierDelayMins = columns[24];
        ret.weatherDelayMins = columns[25];
        ret.nasDelayMins = columns[26];
        ret.securityDelayMins = columns[27];
        ret.lateAircraftDelayMins = columns[28];
        return ret;
    }

    static class FlightEvent {
        String year;
        String month;
        String dayOfMonth;
        String dayOfWeek;
        String depTime;
        String scheduledDepTime;
        String arrTime;
        String scheduledArrTime;
        String uniqueCarrier;
        String flightNum;
        String tailNum;
        String actualElapsedMins;
        String crsElapsedMins;
        String airMins;
        String arrDelayMins;
        String depDelayMins;
        String originAirportCode;
        String destinationAirportCode;
        String distanceInMiles;
        String taxiInTimeMins;
        String taxiOutTimeMins;
        String flightCancelled;
        String cancellationCode; // (A = carrier, B = weather, C = NAS, D = security)
        String diverted; // 1 = yes, 0 = no
        String carrierDelayMins;
        String weatherDelayMins;
        String nasDelayMins;
        String securityDelayMins;
        String lateAircraftDelayMins;

        public int getArrDelayMins() {
            try {
                return Integer.parseInt(arrDelayMins);
            } catch (Throwable e) {
                return 0;
            }
        }

        @Override
        public String toString() {
            return "FlightEvent{" +
                    "month='" + month + '\'' +
                    ", year='" + year + '\'' +
                    '}';
        }
    }

    // TUPLESSSSSS
    static class RecordAggregate {

        String uniqueCarrier;
        int totalMins;
        int count;

        public RecordAggregate(String uniqueCarrier, int totalMins, int count) {
            this.totalMins = totalMins;
            this.count = count;
            this.uniqueCarrier = uniqueCarrier;
        }

        public RecordAggregate plus(RecordAggregate curr) {
            return new RecordAggregate(uniqueCarrier, this.totalMins + curr.totalMins, this.count + curr.count);
        }

        @Override
        public String toString() {
            int avg = totalMins / count;
            return String.format("Delays for carrier %s: %d average mins, %d delayed flights", uniqueCarrier, avg, count);
        }
    }

    static class FlightDelayRecord {
        String year;
        String month;
        String dayOfMonth;
        String flightNum;
        String uniqueCarrier;
        String arrDelayMins;

        public String getUniqueCarrier() {
            return uniqueCarrier;
        }

        public int getArrDelayMins() {
            try {
                return Integer.parseInt(arrDelayMins);
            } catch (Throwable e) {
                return 0;
            }
        }

        @Override
        public String toString() {
            return "FlightDelayRecord{" +
                    "year='" + year + '\'' +
                    ", month='" + month + '\'' +
                    ", dayOfMonth='" + dayOfMonth + '\'' +
                    ", flightNum='" + flightNum + '\'' +
                    ", uniqueCarrier='" + uniqueCarrier + '\'' +
                    ", arrDelayMins='" + arrDelayMins + '\'' +
                    '}';
        }
    }


}
