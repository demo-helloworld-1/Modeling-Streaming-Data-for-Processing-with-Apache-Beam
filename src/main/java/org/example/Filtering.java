package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Filtering {

    public static class FilterThresholdFn extends DoFn<Double,Double>{
        private double threshold = 0;

        public FilterThresholdFn(double threshold){
            this.threshold=threshold;
        }

        @ProcessElement
        public void ProcessElement(ProcessContext c){
            if (threshold < c.element()){
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> googleStockPrice = Arrays.asList(1367.36, 1360.66, 1394.20, 1393.33, 1404.31, 1419.82, 1429.73);

        /*The below 2 line code will be used to just run the transformations but output wont be available in terminal
        pipeline.apply(Create.of(googleStockPrice))
                .apply(ParDo.of(new FilterThresholdFn(1400)));
                */

        pipeline.apply(Create.of(googleStockPrice))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {

                    @Override
                    public Double apply(Double input) {
                        System.out.println("-Pre Filtered = " + input);
                        return input;
                    }
                }))
                .apply(ParDo.of(new FilterThresholdFn(1400)))
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                    @Override
                    public Void apply(Double input){
                        System.out.println("-Post Filtered = "+input);
                        return null;
                    }
                }));
        pipeline.run();
    }
}
