INDIR=new_exp_stream_all

arr=($(find ${INDIR} -path '*.csv'))
echo ${arr[@]}
python produce_result_table.py \
    -i ${arr[@]} \
    -o "$INDIR/combined_results.csv"