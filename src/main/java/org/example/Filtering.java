package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.List;

public class Filtering {

    public static class FilterThresholdFn extends DoFn<Double,Double>{
        private double threshold = 0;

        public FilterThresholdFn(double threshold){
            this.threshold=threshold;
        }

        @ProcessElement
        public void ProcessElement(ProcessContext c){
            if (c.element()>threshold){
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> googleStockPrice = Arrays.asList(1367.36, 1360.66, 1394.20, 1393.33, 1404.31, 1419.82, 1429.73);

        pipeline.apply(Create.of(googleStockPrice))
                .apply(ParDo.of(new FilterThresholdFn(1400)));

        pipeline.run();
    }
}
