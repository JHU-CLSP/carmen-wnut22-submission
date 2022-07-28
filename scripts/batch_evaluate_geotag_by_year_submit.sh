for i in {2013..2021}
do
    qsub batch_evaluate_geotag_by_year.sh $i
done