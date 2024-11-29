from flask import Flask, jsonify, render_template, request
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF, StopWordsRemover
import re
from transformers import pipeline
from apscheduler.schedulers.background import BackgroundScheduler




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
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device = 'mps')
result_data = None
keyword = ""
state = ""
country = ""
model = ""

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
           .withColumn('state', F.lower(F.col('state'))) \
           .withColumn('country', F.lower(F.col('country'))) \
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
    result_data = result_data.repartition(8)
    result_data.count()
    result_data = result_data.persist(StorageLevel.DISK_ONLY)
    print("Spark preprocessing completed.")





@app.route("/")
def home():
    return render_template("home.html")


@app.route('/search', methods = ['POST'])
def search_form():
    global keyword, state, country, model
    keyword = request.form['keyword']
    state = request.form['state']
    country = request.form['country']
    model  = request.form['model']

    keyword = keyword.strip().lower() if keyword else ''
    state = state.strip().lower() if state else ''
    country = country.strip().lower() if country else ''
    keyword = keyword.strip().lower() if keyword else ''


    if result_data is None:
        return jsonify({"error": "Data has not been preprocessed yet. Please try again later."}), 500


    if keyword:
        keyword_result = result_data.filter(F.array_contains(F.col('frequent_terms'), keyword))
    else :
        keyword_result = result_data

    if model != 'Both':
        if 'gpt-3.5' in model :
            keyword_result =  keyword_result.filter(F.col('model').contains('gpt-3.5'))
        else:
            keyword_result = keyword_result.filter(F.col('model').contains('gpt-4'))

    if state != '':
        keyword_result = keyword_result.filter(F.col('state') == state.strip().lower())

    if country != '':
        keyword_result = keyword_result.filter(F.col('country') == country.strip().lower())

    keyword_result = keyword_result.select(F.col('full_interaction'), F.col('clean_interaction'), F.col('state'), F.col('country'), F.col('model'))
    print(keyword, country, state, model)
    keyword_result = keyword_result.withColumn('userprompt', F.split(F.col("full_interaction"), " --botresp-- ").getItem(0)).withColumn('botresp', F.split(F.col('full_interaction'), ' --botresp-- ').getItem(1)).drop('full_interaction')
    result = keyword_result.toPandas().to_dict(orient='records')
    print(result)
    return jsonify(result)


@app.route('/summarize', methods = ['POST'])
def text_summarize():
    text = request.form['text']
    summarized = summarizer(text, min_length = 40, max_length = 1000, do_sample = False)
    return summarized[0]['summary_text']






# Run Flask app
if __name__ == "__main__":
    spark_preprocess()
    app.run()