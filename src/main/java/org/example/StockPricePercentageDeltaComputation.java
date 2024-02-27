package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.*;



public class StockPricePercentageDeltaComputation {
    public static final String CSV_HEADER = "Date,Open,High,Low,Close,Adj Close,Volume,Name";

    // This is the interface which inherits/extends PipelineOption Class, and we modify getInputFile and setInputFile methods
    public interface TotalScoreComputationOptions extends PipelineOptions {

        @Description("Path for the Input file")
        @Default.String("src/main/resources/Source/Streaming_Source/*.csv")
        String getInputFile();

        void setInputFile(String Value);


        @Description("Path for OutPut File")
        @Validation.Required()
        @Default.String("src/main/resources/Sink/Streaming_Sink/percentage_delta")
        String getOutputFile();

        void setOutputFile(String Value);
    }

    public static void main(String[] args) {

        /* Here we created our own custom Options when it has
        ability to take the input and output from command line using args*/
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile())
                        .watchForNewFiles(Duration.standardSeconds(10),
                                Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(30))))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputePriceDeltaPercentage()))
                .apply(ParDo.of(new ConvertIntoStringFn()))

                .apply(ParDo.of(new DoFn<String, Void>() { //This transformation just returns the average value
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        System.out.println(c.element());
                    }
                }))
                /* Tried to Write the data on file but Failed,
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(TextIO.write().to(options.getOutputFile()).withHeader("Date,PriceDelta").withNumShards(1)) */
        ;

        pipeline.run().waitUntilFinish();
    }

    public static class FilterHeaderFn extends DoFn<String, String> {
        private final String header;
        public FilterHeaderFn(String header) {
            this.header = header;
        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }

    }

    public static class ComputePriceDeltaPercentage extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] data = c.element().split(",");

            String date = data[0];

            double openPrice = Double.parseDouble(data[1]);
            double closePrice = Double.parseDouble(data[4]);

            double percentageDelta = ((closePrice-openPrice)/openPrice)*100;
            double percentageDeltaRounded = (double) Math.round(percentageDelta + 100) /100;

            c.output(KV.of(date, percentageDeltaRounded));

        }
    }

    public static class ConvertIntoStringFn extends DoFn<KV<String, Double>, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getKey() + "," + c.element().getValue());
        }

    }


}
