package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.nio.channels.Pipe;
import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/Source/Word_Count/Kinglear.txt"))
                .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply("CountWords", Count.<String>perElement())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) ->
                                wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("src/main/resources/Sink/WordCount").withNumShards(1));

        pipeline.run().waitUntilFinish();

    }
}
