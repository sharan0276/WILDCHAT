
from flask import Flask, jsonify
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF, StopWordsRemover
import re



# Spark session
def defineSparkSession():
    return SparkSession.builder.appName("WILDCHAT-Preprocessing") \
        .config('spark.driver.memory', '32g') \
        .config('spark.executor.memory', '16g') \
        .config('spark.sql.debug.maxToStringFields', 1000) \
        .config("spark.default.parallelism", "24") \
        .config("spark.driver.maxResultSize", "10g") \
        .master('local[8]') \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


# Define Flask app
app = Flask(__name__)
spark = defineSparkSession()
result_data = None

# Custom stop words
custom_stop_words = StopWordsRemover.loadDefaultStopWords("english") + ["\n", "\t", ""]
global_pipeline = Pipeline(stages=[
    Tokenizer(inputCol='clean_interaction', outputCol='tokenized_clean'),
    StopWordsRemover(inputCol='tokenized_clean', outputCol='swr_clean_tokens', stopWords=custom_stop_words),
    CountVectorizer(inputCol='swr_clean_tokens', outputCol='raw_features', vocabSize=100000000, minDF=2.0),
    IDF(inputCol='raw_features', outputCol='tfidf_features')
])

# Clean text function
@F.udf(StringType())
def cleanText(prompt):
    if prompt:
        clean_text = re.sub(r'[^a-zA-Z0-9\.\*/=:,.&|^%@!# ]', '', prompt)
        clean_text = re.sub(r'([\-*/=:,.&|^%@!#])\1+', r' \1 ', clean_text)
        clean_text = re.sub(r'(\d)([a-z])', r'\1 \2', clean_text)
        clean_text = re.sub(r'([.?])', r' \1 ', clean_text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        clean_text = clean_text.replace('/', '')
        return clean_text.lower()
    return ''


# Load data
def load_data(spark):
    return spark.read.parquet('/Users/sharan/Desktop/IDMP Data/*.parquet')

# Preprocess data
def prepare_data(df):
    df = df.filter((F.col('language') == "English") &
                   (F.col('toxic') == False) &
                   (F.col('redacted') == False)) \
           .drop('openai_moderation', 'detoxify_moderation', 'hashed_ip', 'header', 'toxic', 'redacted', 'timestamp') \
           .withColumn('conversation_explode', F.explode(F.col("conversation"))) \
           .withColumn('prompt', F.col('conversation_explode.content')) \
           .withColumn('turn_identifier', F.col('conversation_explode.turn_identifier')) \
           .drop('conversation_explode') \
           .fillna({'state': " ", 'country': " "}) \
           .withColumn('clean', cleanText(F.col('prompt'))) \
           .withColumn('clean', F.trim(F.col('clean'))) \
           .drop('conversation')

    groupCols = [col for col in df.columns if col != 'prompt' and col != 'clean']

    return df.groupBy(groupCols).agg(
        F.concat_ws(' --botresp-- ', F.collect_list('prompt')).alias('full_interaction'),
        F.concat_ws(' ', F.collect_list('clean')).alias('clean_interaction')
    )

# Pipeline definition
def pipeline_definition(df):
    return global_pipeline.fit(df)

# Extract frequent terms
def extract_frequent_terms(tfidf_vector, vocab, threshold=2):
    indices = tfidf_vector.indices
    values = tfidf_vector.values
    terms = [vocab[i] for i, val in zip(indices, values) if val >= threshold and len(vocab[i]) > 1 ]
    return terms

# UDF for frequent terms
def frequent_terms_udf(vocab_broadcast):
    @F.udf(ArrayType(StringType()))
    def udf_function(tfidf_vector):
        return extract_frequent_terms(tfidf_vector, vocab_broadcast.value)
    return udf_function

# Main function to process data

def spark_preprocess():
    global result_data  # Declare global variable
    print("Starting Spark preprocessing...")
    main_df = load_data(spark)
    preprocess_df = prepare_data(main_df)
    preprocess_df  = preprocess_df.persist(StorageLevel.DISK_ONLY)
    pipeline_model = pipeline_definition(preprocess_df)
    processed_data = pipeline_model.transform(preprocess_df)


    # Broadcast vocabulary
    vocab_broadcast = spark.sparkContext.broadcast(pipeline_model.stages[2].vocabulary)

    # Apply frequent terms UDF
    processed_data = processed_data.withColumn(
        "frequent_terms",
        frequent_terms_udf(vocab_broadcast)(F.col("tfidf_features"))
    ).drop('tfidf_features', 'raw_features', 'swr_clean_tokens', 'tokenized_clean')

    # Cache result data
    result_data = processed_data
    preprocess_df.unpersist(blocking=True)
    result_data = result_data.persist(StorageLevel.DISK_ONLY)
    print("Spark preprocessing completed.")

    return result_data

    # Return result (example: limit 2 rows)
    #result_data.limit(2).toPandas().to_json()
    '''result_data.limit(2).show()
    print("\n --------------------------------------------------\n")
    print(result_data.printSchema())'''

@app.route("/")
def home():
    # Serve precomputed data (limit 2 rows)
    data = spark_preprocess()
    result = data.limit(2).toPandas().to_dict(orient='records')
    #print(result)
    x = {
        "s": "ha"
    }
    return jsonify(data)


# Run Flask app
if __name__ == "__main__":
    # spark_preprocess()

    app.run()