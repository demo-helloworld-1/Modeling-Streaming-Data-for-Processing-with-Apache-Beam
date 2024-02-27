package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.util.Arrays;
import java.util.List;

public class Aggregation {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline= Pipeline.create(options);

        List<Double> googleStockPrice = Arrays.asList(1367.36, 1360.66, 1394.20, 1393.33, 1404.31, 1419.82, 1429.73);

        pipeline.apply(Create.of(googleStockPrice))
                .apply(Mean.<Double>globally())
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {

                    @Override
                    public Void apply(Double input){
                        System.out.println("Average Value = "+ input);
                        return null;
                    }
                }));

        pipeline.run();


    }
}
