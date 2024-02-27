package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputation {
    public static final String CSV_HEADER = "Id,Name,Physics,Chemistry,Maths,English,Biology,History";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/Source/student_scores.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertIntoStringFn()))
                .apply(TextIO.write().to("src/main/resources/Sink/student_scores.csv").withHeader("Name,Total").withNumShards(1));
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

    public static class ConvertIntoStringFn extends  DoFn<KV<String,Integer>,String >{

        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(c.element().getKey()+","+c.element().getValue());
        }

    }


}
