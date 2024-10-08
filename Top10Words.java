import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10Words {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: top10words <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "top 10 words");
        job.setJarByClass(Top10Words.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Top10Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Top10Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        job.setNumReduceTasks(1);  // set single reducer for final op

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper class
    public static class Top10Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable count = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\t");
            if (tokens.length == 2) {
                word.set(tokens[0]);
                count.set(Integer.parseInt(tokens[1]));
                context.write(word, count);
            }
        }
    }

    // Reducer class for selecting top 10 words
    public static class Top10Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<WordCountPair> queue = new PriorityQueue<>();
        private static final int TOP_N = 10;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            queue.add(new WordCountPair(key.toString(), sum));
            if (queue.size() > TOP_N) {
                queue.poll();
            }
        }

        // Cleanup method to write the top 10 words in descending order
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                WordCountPair pair = queue.poll();
                context.write(new Text(pair.word), new IntWritable(pair.count));
            }
        }

        private static class WordCountPair implements Comparable<WordCountPair> {
            String word;
            int count;

            WordCountPair(String word, int count) {
                this.word = word;
                this.count = count;
            }

            public int compareTo(WordCountPair pair) {
                return Integer.compare(this.count, pair.count);
            }
        }
    }
}