package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

public class FixedWindow {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create();

        PCollection<String> carMakesTimes = pipeline.apply(Create.timestamped(
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:05").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:06").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:07").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:08").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:11").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:12").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:13").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:14").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:16").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:17").toInstant())));

        PCollection<String> windowedMakesTimes = carMakesTimes.apply(
                "Window", Window.into(FixedWindows.of(Duration.standardSeconds(5))));


        PCollection<KV<String,Long>> output = windowedMakesTimes.apply(Count.perElement());

        output.apply(ParDo.of(new DoFn<KV<String,Long>,String> (){

            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window){


                /*In Terminal Results
                System.out.println(String.format(
                        "%s: %s %s", window.maxTimestamp(), c.element(). getKey(),c.element().getValue()));

                 */


                /*Tried a new Code to create a file by altering the Timestamp but didnt resolved, so we need to use other file systems
                like HDFS or GoogleCloudStorage*/
                //DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'");
               // String formattedTimestamp = formatter.print(window.maxTimestamp());


                c.output(String.format("%s %s %s",window.maxTimestamp(), Objects.requireNonNull(c.element()).getKey(), Objects.requireNonNull(c.element()).getValue()));

            }
        })).apply(TextIO.write().to("src/main/resources/Sink/FixedWindow/output").withWindowedWrites().withNumShards(1));
                                /*
                                If you use Windows platform the file/folder name with ":" are not supported, so you will end up with error.
                                Ignore it and refer the temp files output.
                                 */

        pipeline.run().waitUntilFinish();

    }
}
