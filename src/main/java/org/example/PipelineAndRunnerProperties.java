//Introduction to Apache Beam
//1st Program

package org.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineAndRunnerProperties {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Runner = "+ options.getRunner().getName());
        System.out.println("JobName ="+ options.getJobName());
        System.out.println("Option ID ="+options.getOptionsId());
        System.out.println("Stable Unique Name = "+options.getStableUniqueNames());
        System.out.println("Temp Location = "+options.getTempLocation());
        System.out.println("User Agent = "+options.getUserAgent());
    }
}