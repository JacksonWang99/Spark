#Creat by Zhenming Wang   z5140192


from pyspark.ml.feature import Tokenizer, CountVectorizer, StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline

#   id|category| descript  data structure
def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category",
                               output_feature_col="features", output_label_col="label"):

    # tokenizer:translate the descript to words(list_descript)
    tokenizer = Tokenizer(inputCol=input_descript_col, outputCol='list_descript')
    # words(list_descript) to features
    cv = CountVectorizer(inputCol='list_descript', outputCol=output_feature_col)
    #category to label
    indexer = StringIndexer(inputCol=input_category_col, outputCol=output_label_col)
    #Build Pipeline
    first_pipeline = Pipeline(stages=[tokenizer, cv, indexer])
    return first_pipeline



def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):

    data_select_number = training_df.select("group").distinct().count()
    condition = 0
    for i in range(data_select_number):
        #creat train_data, test_data
        train_data, test_data = creat_train_test_data(training_df, i)
        build_pipeline = Pipeline(stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2])
        #In fact, this step is over. On the basis of the original table, 18 columns have been added

        if condition == 0:
            nb_svm_pred_six = build_pipeline.fit(train_data).transform(test_data)
            condition = 1
        else:
            nb_svm_pred_six = nb_svm_pred_six.union(build_pipeline.fit(train_data).transform(test_data))

    #Binary to decimal

    f = udf(Binary_to_decimal, DoubleType())

    # create joint_pred_x column
    join_pred_nine1 = nb_svm_pred_six.withColumn("joint_pred_0", f("nb_pred_0", "svm_pred_0"))
    join_pred_nine2 = join_pred_nine1.withColumn("joint_pred_1", f("nb_pred_1", "svm_pred_1"))
    join_pred_nine3 = join_pred_nine2.withColumn("joint_pred_2", f("nb_pred_2", "svm_pred_2"))
    return join_pred_nine3


def creat_train_test_data(training_df, i):
    groupcondition = training_df["group"] == i
    train_data = training_df.where(~groupcondition)
    test_data = training_df.where(groupcondition)
    return train_data, test_data
    
def Binary_to_decimal(nb_pre,svm_pre):
    # change to int type
    a = int(nb_pre)
    b = int(svm_pre)
    #change to string type and put them toghter
    c = str(a)+str(b)
    #Binary to decimal
    d = int(c, 2)
    e = float(d)
    return e


#After the 9 columns we need are generated, it is the input of the third question

def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model,
                    gen_meta_feature_pipeline_model, meta_classifier):

    label_x = base_features_pipeline_model.transform(test_df)
    nb_svm_pred_six = gen_base_pred_pipeline_model.transform(label_x)
    f = udf(Binary_to_decimal, DoubleType())

    join_pred_nine1 = nb_svm_pred_six.withColumn("joint_pred_0", f("nb_pred_0", "svm_pred_0"))
    join_pred_nine2 = join_pred_nine1.withColumn("joint_pred_1", f("nb_pred_1", "svm_pred_1"))
    join_pred_nine3 = join_pred_nine2.withColumn("joint_pred_2", f("nb_pred_2", "svm_pred_2"))

    featuresDF = gen_meta_feature_pipeline_model.transform(join_pred_nine3)
    featuresDF_table = meta_classifier.transform(featuresDF)
    output_Datafram = featuresDF_table.select("id", "label", "final_prediction")
    return output_Datafram







