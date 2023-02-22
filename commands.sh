# /usr/bin/env bash
hadoop fs -rm -r 1_grams ; 
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_count_reducer.py \
-input hw1.2/* \
-output 1_grams \
-mapper "python n_gram_count_mapper.py 1 3" \
-reducer "python n_gram_count_reducer.py" \
&& hadoop fs -rm -r 1_grams_sum; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_sum_reducer.py \
-input 1_grams/* \
-output 1_grams_sum \
-mapper "cat" \
-combiner "python n_gram_sum_reducer.py" \
-numReduceTasks 1 \
-reducer "python n_gram_sum_reducer.py" \
&& hadoop fs -rm -r 1_grams_prob; \
mapred streaming -file uni_gram_prob_mapper.py \
-input 1_grams/* \
-output 1_grams_prob \
-mapper "python uni_gram_prob_mapper.py $(hadoop fs -cat 1_grams_sum\/*  | cut -f 2)" \
-reducer "cat" ;

hadoop fs -rm -r 2_grams ; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_count_reducer.py \
-input hw1.2/* \
-output 2_grams \
-mapper "python n_gram_count_mapper.py 2 3" \
-reducer "python n_gram_count_reducer.py" \
&& hadoop fs -rm -r 2_grams_sum; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_sum_reducer.py \
-input 2_grams/* \
-output 2_grams_sum \
-mapper "cat" \
-numReduceTasks 1 \
-combiner "python n_gram_sum_reducer.py" \
-reducer "python n_gram_sum_reducer.py " ;


hadoop fs -rm -r 2_grams_prob; \
mapred streaming \
-D stream.num.map.output.key.fields=2 \
-D mapred.text.key.partitioner.options="-k1,1" \
-D mapred.text.key.comparator.options="-k1,2" \
-file n_gram_prob_mapper.py \
-file n_gram_prob_reducer.py \
-input 2_grams/* \
-input 1_grams_prob/* \
-output 2_grams_prob \
-mapper "python n_gram_prob_mapper.py 2 $(hadoop fs -cat 2_grams_sum\/*  | cut -f 2)" \
-reducer "python n_gram_prob_reducer.py" \
--partitioner "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner" ;





hadoop fs -rm -r 3_grams ; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_count_reducer.py \
-input hw1.2/* \
-output 3_grams \
-mapper "python n_gram_count_mapper.py 3 3" \
-reducer "python n_gram_count_reducer.py" \
&& hadoop fs -rm -r 3_grams_sum; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_sum_reducer.py \
-input 3_grams/* \
-output 3_grams_sum \
-mapper "cat" \
-numReduceTasks 1 \
-reducer "python n_gram_sum_reducer.py " ;

hadoop fs -rm -r 3_grams_prob; \
mapred streaming \
-D stream.num.map.output.key.fields=2 \
-D mapred.text.key.partitioner.options="-k1,1" \
-D mapred.text.key.comparator.options="-k1,2" \
-file n_gram_prob_mapper.py \
-file n_gram_prob_reducer.py \
-input 3_grams/* \
-input 2_grams_prob/* \
-output 3_grams_prob \
-mapper "python n_gram_prob_mapper.py 3 $(hadoop fs -cat 3_grams_sum\/*  | cut -f 2)" \
-reducer "python n_gram_prob_reducer.py" \
--partitioner "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner";



hadoop fs -rm -r 3_grams_sorted; mapred streaming \
-D stream.num.map.output.key.fields=3 \
-D mapred.text.key.partitioner.options="-k1,1" \
-D mapred.text.key.comparator.options="-k1,2" \
-file n_gram_sort_mapper.py \
-input 3_grams_prob/* \
-mapper "python n_gram_sort_mapper.py" \
-output 3_grams_sorted \
-reducer "cat" \
--partitioner "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner" ;


hadoop fs -cat 3_grams_sorted/* | grep -P "^united states\t" | tail -n 1;