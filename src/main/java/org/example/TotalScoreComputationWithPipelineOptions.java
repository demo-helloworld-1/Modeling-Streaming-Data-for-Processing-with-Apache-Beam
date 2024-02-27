package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputationWithPipelineOptions {
    public static final String CSV_HEADER = "Id,Name,Physics,Chemistry,Maths,English,Biology,History";

    public interface TotalScoreComputationOptions extends PipelineOptions {

        @Description("Path for the Input file")
        @Default.String("src/main/resources/Source/student_scores.csv")
        String getInputFile();

        void setInputFile(String Value);


        @Description("Path for OutPut File")
        @Validation.Required()
        String getOutputFile();

        void setOutputFile(String Value);
    }

    public static void main(String[] args) {
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        System.out.println("****Input File: "+options.getInputFile());
        System.out.println("****Output File: "+options.getOutputFile());

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertIntoStringFn()))
                .apply(TextIO.write().to(options.getOutputFile()).withHeader("Name,Total").withNumShards(1));
        /* if you provide withNumShards as 1 it provides all the output data in one file*/
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

    public static class ComputeTotalScoreFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] data = c.element().split(",");

            String name = data[1];

            Integer totalscore = Integer.parseInt(data[2]) +
                    Integer.parseInt(data[3]) + Integer.parseInt(data[4]) +
                    Integer.parseInt(data[5]) + Integer.parseInt(data[6]) + Integer.parseInt(data[7]);

            c.output(KV.of(name, totalscore));

        }
    }

    public static class ConvertIntoStringFn extends DoFn<KV<String, Integer>, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getKey() + "," + c.element().getValue());
        }

    }


}
