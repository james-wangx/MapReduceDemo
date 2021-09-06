# MapReduceDemo

# MapReduce 演示案例

尚硅谷最帅的男人的视频：[https://www.bilibili.com/video/BV1Qp4y1n7EN](https://www.bilibili.com/video/BV1Qp4y1n7EN)

# 入门案例 - 词频统计

## 一、MapReduce 程序简单结构
MR（即 MapReduce，以下简称 MR）程序简单可以分为三个部分：`Mapper，Reducer，Driver`。用户需要自定义两个类（两个文件），分别继承 MR API 的 Mapper 和 Reducer 类，并编写 main 函数入口即 Driver 文件。其中 Mapper 文件和 Reducer 文件分别对应 MR 程序的map 和 reduce 阶段，Driver 文件用来配置任务，如指定输入输出类型，输入输出路径等。


## 二、WordCountMapper
先看一下完整代码，然后再逐个解释
```java
package com.pineapple.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map 阶段的输入和输出数据都是 KV 键值对的形式，KV 的类型可以自定义
 * KEYIN：map阶段输入的key的类型：LongWritable
 * VALUEIN：map阶段输入value类型：Text
 * KEYOUT, map阶段输出key类型：Text
 * VALUEOUT, map阶段输出value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);
    private final String regex = ".*?(\\w+-?\\w+).*?";
    private Pattern pattern;
    private Matcher matcher;

    /**
     * 重写 map 方法，每个 KV 键值对都会调用一次这个方法
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // value 为这一行的数据，Text 类型可以转换成 Java 中的 String 类型以获取更多有关字符串的操作
        String line = value.toString();

        // 按照空格切割字符串
        String[] words = line.split(" ");

        // 循环写入环形缓冲区
        for (String word : words) {
            // 过滤掉标点符号（不包括'-'）
            pattern = Pattern.compile(regex);
            matcher = pattern.matcher(word);
            if (matcher.matches()) {
                String group = matcher.group(1);
                // 封装outK，即将 String 转小写再转为 Text 类型
                outK.set(group.toLowerCase());
                // 写入，参数类型和输出的 KV 类型一致
                context.write(outK, outV);
            }
        }
    }
}
```

自定义一个 Java 类文件，名为 WordCountMapper。

```java
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) {
		......
	}
}
```

继承的 Mapper 有四个泛型，分别对应 `map 阶段的输入 KV 和 输出 KV`，它们都是 Hadoop 自身封装的序列化类型，和 Java 的数据类型类似，LongWritable ~ Long，Text ~ String，IntWritable ~ Integer。

> 注意：
> 	- 因为 Hadoop 有3个版本，所以有些类名是重复的，导包的时候很容易导错
> 	Hadoop2.x 和 3.x 的包都是 `org.apache.hadoop.mapreduce`，Hadoop1.x 的包是 `org.apache.hadoop.mapred`
> 	- LongWritable, Text, IntWritable 等这些数据类型都在 `org.apache.hadoop.io` 包下

具体的业务逻辑代码再 map 方法下编写，key 和 value 为输入的键值对，它们的类型和 Mapper 的前两个泛型是一致的，context 可以解释为上下文对象，主要用来将数据写入环形缓冲区（数据并不是直接交给 Reducer 处理的，在这之间还有个 Shuffle 阶段，环形缓冲区是用来暂存数据的）。

关于输入文件，我选的是个比较复杂的，内容来自 Hadoop MR 官方文档的 Overview 部分：[https://hadoop.apache.org/docs/r3.1.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html](https://hadoop.apache.org/docs/r3.1.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
```
Hadoop MapReduce is a software framework for easily writing applications which process vast amounts of data (multi-terabyte data-sets) in-parallel on large clusters (thousands of nodes) of commodity hardware in a reliable, fault-tolerant manner.
A MapReduce job usually splits the input data-set into independent chunks which are processed by the map tasks in a completely parallel manner. The framework sorts the outputs of the maps, which are then input to the reduce tasks. Typically both the input and the output of the job are stored in a file-system. The framework takes care of scheduling tasks, monitoring them and re-executes the failed tasks.
Typically the compute nodes and the storage nodes are the same, that is, the MapReduce framework and the Hadoop Distributed File System (see HDFS Architecture Guide) are running on the same set of nodes. This configuration allows the framework to effectively schedule tasks on the nodes where data is already present, resulting in very high aggregate bandwidth across the cluster.
The MapReduce framework consists of a single master ResourceManager, one worker NodeManager per cluster-node, and MRAppMaster per application (see YARN Architecture Guide).
Minimally, applications specify the input/output locations and supply map and reduce functions via implementations of appropriate interfaces and/or abstract-classes. These, and other job parameters, comprise the job configuration.
The Hadoop job client then submits the job (jar/executable etc.) and configuration to the ResourceManager which then assumes the responsibility of distributing the software/configuration to the workers, scheduling tasks and monitoring them, providing status and diagnostic information to the job-client.
Although the Hadoop framework is implemented in Java™, MapReduce applications need not be written in Java.
Hadoop Streaming is a utility which allows users to create and run jobs with any executables (e.g. shell utilities) as the mapper and/or the reducer.
Hadoop Pipes is a SWIG-compatible C++ API to implement MapReduce applications (non JNI™ based).
```
可以看到文件中有很多标点符号，和带 '-' 的单词，现在我们的业务需求是过滤掉标点符号，保留带 '-' 的单词，最终统计词频。

我们重点操作的是 value 对象，它代表的是文件中的一行数据。由于 Hadoop 的 Text 类型对字符串的操作比较少，所以我们将其转为 String 类型进行操作。随后按照空格将这一行切割成一个个单词。
```java
// value 为这一行的数据，Text 类型可以转换成 Java 中的 String 类型以获取更多有关字符串的操作
String line = value.toString();
// 按照空格切割字符串
String[] words = line.split(" ");
```
用 for each 语句遍历这一行的每个单词，用正则表达式过滤后再转为小写。最后写入环形缓冲区。
```java
// 循环写入环形缓冲区
for (String word : words) {
    // 过滤掉标点符号（不包括'-'）
    pattern = Pattern.compile(regex);
    matcher = pattern.matcher(word);
    if (matcher.matches()) {
        String group = matcher.group(1);
        // 封装outK，即将 String 转小写再转为 Text 类型
        outK.set(group.toLowerCase());
        // 写入，参数类型和输出的 KV 类型一致
        context.write(outK, outV);
    }
}
```
> 注意：
> - 最终 context 写入的数据类型必须和 Mapper 的后两个泛型一致
> - map 方法每读取一行就会调用一次，为了避免内存开销，可以将 Text，IntWritable 等一些类型的声明放在 map 方法外


## 三、WordCountReducer
先看一下完整代码，再逐个解释
```java
package com.pineapple.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce 阶段的输入数据类型要和 Map 阶段的输出数据类型对应
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN, reduce阶段输入value类型：IntWritable
 * KEYOUT, reduce阶段输出key类型：Text
 * VALUEOUT, reduce阶段输出value类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable outV = new IntWritable();

    /**
     * 每一个 Key 都会调用此方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context
            context) throws IOException, InterruptedException {

        int sum = 0;

        // 值 values 是一个迭代器，可以使用 for each 循环取出每一项
        for (IntWritable value : values)
            sum += value.get();

        outV.set(sum);

        // 写入文件
        context.write(key, outV);
    }
}
```
自定义一个 Java 类文件，类名为 WordCountReducer。
```java
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context
            context) {
		......
	}
}
```
Reduer 的前两个泛型为之前 Mapper 输出的两个类型，后两个类型为最终写入结果文件的类型。
这里的数据在经过 Shuffle 阶段后，已经由<key, 1>的形式变成了<key, <1, 1, 1>>的形式，所以 values 是一个`可迭代对象`，我们可以用 for each 循环将每个1加起来得到最终的单词频率。
```java
int sum = 0;

// 值 values 是一个迭代器，可以使用 for each 循环取出每一项
for (IntWritable value : values)
    sum += value.get();

outV.set(sum);

// 写入文件
context.write(key, outV);
```
> 注意：
> - 每一个键不同的键值对都对调用一次 reduce 方法，为了避免内存开销，可以将数据类型的定义放在 reduce 方法外。
> - context 写入的数据类型和 Reducer 的后两个泛型的类型一致。


## 四、WordCountDriver
完整代码
```java
package com.pineapple.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 创建一个新的 job
        // 如果要 new 的 Configuration() 为空，则可以直接调用 getInstance()
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);

        // 指定一下 job 名
        job.setJobName("WordCount");

        // 关联 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置 map 输出的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 判断输出路径是否存在，存在则删除
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath = new Path("output/WordCount");
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("input/WordCount"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
```
Driver 的代码都是一些固定格式，要讲的基本都在注释里了。Driver 主要还是起到了一个配置的作用，自定义的 Mapper 和 Reducer ，输入输出数据类型，输入输出路径等是要指定一下的，不然 MR 找不到它们。将它们的 `class` 传入，在运行 MR 程序时，Java 会通过`反射的机制`调用我们写的代码。


## 五、跑一下
直接运行 Driver 后会在输出路径产出结果文件，内容大概如下：
```
abstract-classes	1
across	1
aggregate	1
allows	2
already	1
although	1
amounts	1
and	14
......
```
如果是要提交的集群上运行，可以利用一下参数 `args` ，通过指定参数设置文件的输入输出路径：
```java
// 判断输出路径是否存在，存在则删除
Path inputPath = new Path(args[0]);
Path outputPath = new Path(args[1]);
FileSystem fileSystem = FileSystem.get(conf);
if (fileSystem.exists(outputPath))
    fileSystem.delete(outputPath, true);

// 设置输入路径和输出路径
FileInputFormat.setInputPaths(job, inputPath);
FileOutputFormat.setOutputPath(job, outputPath);
```

打包到集群上运行，结果如下：
```bash
hadoop jar MapReduceDemo-1.0-SNAPSHOT.jar com.pineapple.mapreduce.wordcount.HdfsWordCountDriver hdfs://hadoop102:8020/input/WordCount hdfs://hadoop102:8020/output/WordCount
```
![在这里插入图片描述](https://img-blog.csdnimg.cn/b5ceff9ca32c4d7aad0fa1ba5031b798.png?x-oss-process=image/watermark,type_ZHJvaWRzYW5zZmFsbGJhY2s,shadow_50,text_Q1NETiBAcGluZWFwcGxlX3B5,size_20,color_FFFFFF,t_70,g_se,x_16)