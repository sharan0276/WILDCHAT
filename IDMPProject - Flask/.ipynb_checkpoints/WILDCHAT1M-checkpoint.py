{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "b1a129ea-f147-447c-99ee-4ede3a6e0c69",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark import SparkContext, StorageLevel\n",
    "from pyspark.sql import SparkSession\n",
    "import pyspark.sql.functions as F\n",
    "from pyspark.ml import Pipeline\n",
    "from pyspark.sql.types import ArrayType, StringType, BooleanType, IntegerType, StructType, StructField, FloatType\n",
    "from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF, StopWordsRemover\n",
    "from transformers import pipeline\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "7a89c86d-932f-424e-b50c-454319ea3cd7",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:29:55 WARN Utils: Your hostname, Sharans-MacBook-Pro.local resolves to a loopback address: 127.0.0.1; using 10.0.0.28 instead (on interface en0)\n",
      "24/11/23 13:29:55 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address\n",
      "Setting default log level to \"WARN\".\n",
      "To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).\n",
      "24/11/23 13:29:56 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "\n",
       "            <div>\n",
       "                <p><b>SparkSession - in-memory</b></p>\n",
       "                \n",
       "        <div>\n",
       "            <p><b>SparkContext</b></p>\n",
       "\n",
       "            <p><a href=\"http://10.0.0.28:4040\">Spark UI</a></p>\n",
       "\n",
       "            <dl>\n",
       "              <dt>Version</dt>\n",
       "                <dd><code>v3.5.3</code></dd>\n",
       "              <dt>Master</dt>\n",
       "                <dd><code>local[8]</code></dd>\n",
       "              <dt>AppName</dt>\n",
       "                <dd><code>WILDCHAT-1M</code></dd>\n",
       "            </dl>\n",
       "        </div>\n",
       "        \n",
       "            </div>\n",
       "        "
      ],
      "text/plain": [
       "<pyspark.sql.session.SparkSession at 0x16a3c42c0>"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark = SparkSession.builder.appName(\"WILDCHAT-1M\") \\\n",
    "    .config('spark.driver.memory', '24g') \\\n",
    "    .config('spark.executor.memory', '12g') \\\n",
    "    .config('spark.sql.debug.maxToStringFields', 1000) \\\n",
    "    .config(\"spark.default.parallelism\", \"10\") \\\n",
    "    .master('local[8]') \\\n",
    "    .config(\"spark.driver.maxResultSize\", \"10g\") \\\n",
    "    .getOrCreate()\n",
    "spark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "c9ec2b58-9001-4f08-9062-54376c0f4074",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "990372"
      ]
     },
     "execution_count": 3,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "main_df = spark.read.parquet('/Users/sharan/Desktop/IDMP Data/*.parquet')\n",
    "main_df.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "993bc45b-6504-4c91-b49a-e796855da211",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- conversation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- content: string (nullable = true)\n",
      " |    |    |-- country: string (nullable = true)\n",
      " |    |    |-- hashed_ip: string (nullable = true)\n",
      " |    |    |-- header: struct (nullable = true)\n",
      " |    |    |    |-- accept-language: string (nullable = true)\n",
      " |    |    |    |-- user-agent: string (nullable = true)\n",
      " |    |    |-- language: string (nullable = true)\n",
      " |    |    |-- redacted: boolean (nullable = true)\n",
      " |    |    |-- role: string (nullable = true)\n",
      " |    |    |-- state: string (nullable = true)\n",
      " |    |    |-- timestamp: timestamp (nullable = true)\n",
      " |    |    |-- toxic: boolean (nullable = true)\n",
      " |    |    |-- turn_identifier: long (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- openai_moderation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- categories: struct (nullable = true)\n",
      " |    |    |    |-- harassment: boolean (nullable = true)\n",
      " |    |    |    |-- harassment/threatening: boolean (nullable = true)\n",
      " |    |    |    |-- harassment_threatening: boolean (nullable = true)\n",
      " |    |    |    |-- hate: boolean (nullable = true)\n",
      " |    |    |    |-- hate/threatening: boolean (nullable = true)\n",
      " |    |    |    |-- hate_threatening: boolean (nullable = true)\n",
      " |    |    |    |-- self-harm: boolean (nullable = true)\n",
      " |    |    |    |-- self-harm/instructions: boolean (nullable = true)\n",
      " |    |    |    |-- self-harm/intent: boolean (nullable = true)\n",
      " |    |    |    |-- self_harm: boolean (nullable = true)\n",
      " |    |    |    |-- self_harm_instructions: boolean (nullable = true)\n",
      " |    |    |    |-- self_harm_intent: boolean (nullable = true)\n",
      " |    |    |    |-- sexual: boolean (nullable = true)\n",
      " |    |    |    |-- sexual/minors: boolean (nullable = true)\n",
      " |    |    |    |-- sexual_minors: boolean (nullable = true)\n",
      " |    |    |    |-- violence: boolean (nullable = true)\n",
      " |    |    |    |-- violence/graphic: boolean (nullable = true)\n",
      " |    |    |    |-- violence_graphic: boolean (nullable = true)\n",
      " |    |    |-- category_scores: struct (nullable = true)\n",
      " |    |    |    |-- harassment: double (nullable = true)\n",
      " |    |    |    |-- harassment/threatening: double (nullable = true)\n",
      " |    |    |    |-- harassment_threatening: double (nullable = true)\n",
      " |    |    |    |-- hate: double (nullable = true)\n",
      " |    |    |    |-- hate/threatening: double (nullable = true)\n",
      " |    |    |    |-- hate_threatening: double (nullable = true)\n",
      " |    |    |    |-- self-harm: double (nullable = true)\n",
      " |    |    |    |-- self-harm/instructions: double (nullable = true)\n",
      " |    |    |    |-- self-harm/intent: double (nullable = true)\n",
      " |    |    |    |-- self_harm: double (nullable = true)\n",
      " |    |    |    |-- self_harm_instructions: double (nullable = true)\n",
      " |    |    |    |-- self_harm_intent: double (nullable = true)\n",
      " |    |    |    |-- sexual: double (nullable = true)\n",
      " |    |    |    |-- sexual/minors: double (nullable = true)\n",
      " |    |    |    |-- sexual_minors: double (nullable = true)\n",
      " |    |    |    |-- violence: double (nullable = true)\n",
      " |    |    |    |-- violence/graphic: double (nullable = true)\n",
      " |    |    |    |-- violence_graphic: double (nullable = true)\n",
      " |    |    |-- flagged: boolean (nullable = true)\n",
      " |-- detoxify_moderation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- identity_attack: double (nullable = true)\n",
      " |    |    |-- insult: double (nullable = true)\n",
      " |    |    |-- obscene: double (nullable = true)\n",
      " |    |    |-- severe_toxicity: double (nullable = true)\n",
      " |    |    |-- sexual_explicit: double (nullable = true)\n",
      " |    |    |-- threat: double (nullable = true)\n",
      " |    |    |-- toxicity: double (nullable = true)\n",
      " |-- toxic: boolean (nullable = true)\n",
      " |-- redacted: boolean (nullable = true)\n",
      " |-- state: string (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- header: struct (nullable = true)\n",
      " |    |-- accept-language: string (nullable = true)\n",
      " |    |-- user-agent: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "main_df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "08b5db3d-d82f-4264-8586-7a503de52d25",
   "metadata": {},
   "outputs": [],
   "source": [
    "copy_df = main_df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "34370b8e-b2de-4bcf-ac2e-a8716bba0067",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- conversation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- content: string (nullable = true)\n",
      " |    |    |-- country: string (nullable = true)\n",
      " |    |    |-- hashed_ip: string (nullable = true)\n",
      " |    |    |-- header: struct (nullable = true)\n",
      " |    |    |    |-- accept-language: string (nullable = true)\n",
      " |    |    |    |-- user-agent: string (nullable = true)\n",
      " |    |    |-- language: string (nullable = true)\n",
      " |    |    |-- redacted: boolean (nullable = true)\n",
      " |    |    |-- role: string (nullable = true)\n",
      " |    |    |-- state: string (nullable = true)\n",
      " |    |    |-- timestamp: timestamp (nullable = true)\n",
      " |    |    |-- toxic: boolean (nullable = true)\n",
      " |    |    |-- turn_identifier: long (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- toxic: boolean (nullable = true)\n",
      " |-- redacted: boolean (nullable = true)\n",
      " |-- state: string (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "copy_df = copy_df.drop('openai_moderation', 'detoxify_moderation', 'header')\n",
    "copy_df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "1e54b07c-7715-435c-8774-74b79805b698",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[Row(redacted=True)]"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "copy_df.filter(F.col('redacted') == True).select(F.col('redacted')).distinct().collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "9343db7f-7a5a-4f0f-bfa9-09310e4e72bc",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "copy_df = copy_df.filter((F.col('language') == \"English\") & \n",
    "               (F.col('toxic') == False) & \n",
    "               (F.col('redacted') == False))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "ec59c24a-e997-432d-9b77-5bb63203a7cf",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "473265"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "copy_df.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "1119cdee-b937-4926-a15f-55ba62effbe6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- conversation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- content: string (nullable = true)\n",
      " |    |    |-- country: string (nullable = true)\n",
      " |    |    |-- hashed_ip: string (nullable = true)\n",
      " |    |    |-- header: struct (nullable = true)\n",
      " |    |    |    |-- accept-language: string (nullable = true)\n",
      " |    |    |    |-- user-agent: string (nullable = true)\n",
      " |    |    |-- language: string (nullable = true)\n",
      " |    |    |-- redacted: boolean (nullable = true)\n",
      " |    |    |-- role: string (nullable = true)\n",
      " |    |    |-- state: string (nullable = true)\n",
      " |    |    |-- timestamp: timestamp (nullable = true)\n",
      " |    |    |-- toxic: boolean (nullable = true)\n",
      " |    |    |-- turn_identifier: long (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- toxic: boolean (nullable = true)\n",
      " |-- redacted: boolean (nullable = true)\n",
      " |-- state: string (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "copy_df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "6c645d85-941e-4583-a8ab-6246aeffc1b2",
   "metadata": {},
   "outputs": [],
   "source": [
    "copy_df = copy_df.drop('toxic', 'redacted')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "b7081180-e8b8-4f14-a751-c9779fc8fc5e",
   "metadata": {},
   "outputs": [],
   "source": [
    "copy_df = copy_df.withColumn('conversation_explode', F.explode(F.col(\"conversation\"))) \\\n",
    "    .withColumn('prompt', F.col('conversation_explode.content')) \\\n",
    "    .withColumn('turn_identifier', F.col('conversation_explode.turn_identifier')) \\\n",
    "    .drop('conversation_explode')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "b71902f4-cdf8-4a78-bf99-a4d904165001",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- conversation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- content: string (nullable = true)\n",
      " |    |    |-- country: string (nullable = true)\n",
      " |    |    |-- hashed_ip: string (nullable = true)\n",
      " |    |    |-- header: struct (nullable = true)\n",
      " |    |    |    |-- accept-language: string (nullable = true)\n",
      " |    |    |    |-- user-agent: string (nullable = true)\n",
      " |    |    |-- language: string (nullable = true)\n",
      " |    |    |-- redacted: boolean (nullable = true)\n",
      " |    |    |-- role: string (nullable = true)\n",
      " |    |    |-- state: string (nullable = true)\n",
      " |    |    |-- timestamp: timestamp (nullable = true)\n",
      " |    |    |-- toxic: boolean (nullable = true)\n",
      " |    |    |-- turn_identifier: long (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = true)\n",
      " |-- country: string (nullable = true)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- prompt: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "copy_df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "5139abfc-90da-4b55-8106-6d55d0ab41b3",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "data": {
      "text/plain": [
       "0"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "copy_df.filter(F.col('prompt').isNull()).count() # no prompt is empty, the data is clean"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "7c588d99-1159-4504-9e99-d171ba9d93ba",
   "metadata": {},
   "outputs": [],
   "source": [
    "process_data = copy_df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "fd365236-f5a5-47dc-bae7-d0112efe189d",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-----------------+-----+---------+------------+----+--------+------+-------+---------+------+---------------+\n",
      "|conversation_hash|model|timestamp|conversation|turn|language| state|country|hashed_ip|prompt|turn_identifier|\n",
      "+-----------------+-----+---------+------------+----+--------+------+-------+---------+------+---------------+\n",
      "|                0|    0|        0|           0|   0|       0|289644|   1990|        0|     0|              0|\n",
      "+-----------------+-----+---------+------------+----+--------+------+-------+---------+------+---------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Preprocess 1 : Check for null values\n",
    "\n",
    "null_counts = process_data.select([F.sum(F.col(c).isNull().cast(\"int\")).alias(c) for c in process_data.columns])\n",
    "null_counts.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "848cf59a-7356-44f6-98fa-d86c114e5658",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 16:========================>                               (16 + 8) / 37]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-----------------+-----+---------+------------+----+--------+-----+-------+---------+------+---------------+\n",
      "|conversation_hash|model|timestamp|conversation|turn|language|state|country|hashed_ip|prompt|turn_identifier|\n",
      "+-----------------+-----+---------+------------+----+--------+-----+-------+---------+------+---------------+\n",
      "|                0|    0|        0|           0|   0|       0|    0|      0|        0|     0|              0|\n",
      "+-----------------+-----+---------+------------+----+--------+-----+-------+---------+------+---------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "#Preprocess 2 : Update Null Values to empty strings\n",
    "process_data = process_data.fillna({'state' : \" \", 'country' : \" \"})\n",
    "process_data.select([F.sum(F.col(c).isNull().cast(\"int\")).alias(c) for c in process_data.columns]).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "6c00071e-df20-42f2-a575-64102688c772",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Preprocess 3 : Cleaning the prompt to remove all special characters.\n",
    "\n",
    "import re\n",
    "\n",
    "@F.udf(StringType())\n",
    "def cleanText(prompt):\n",
    "    if prompt:\n",
    "        #clean_text = re.sub('[^a-zA-Z0-9]', '', prompt)\n",
    "        #clean_text = re.sub(' \\\\-*/=:,.&|^%$@!%', r'\\1', clean_text)\n",
    "        # Remove all non-alphanumeric characters except specific ones (.-/=,:,&|^%@!%)\n",
    "        clean_text = re.sub(r'[^a-zA-Z0-9\\.\\*/=:,.&|^%@! ]', '', prompt)\n",
    "        clean_text = clean_text.lower()\n",
    "        # Replace multiple consecutive special characters with a single one\n",
    "        clean_text = re.sub(r'([\\-*/=:,.&|^%@!])', r' \\1 ', clean_text)\n",
    "        clean_text = re.sub(r'(\\d)([a-z])', r'\\1 \\2', clean_text)\n",
    "        clean_text = re.sub(r'([.?])', r' \\1 ', clean_text)\n",
    "        clean_text = re.sub(r'\\s+', ' ', clean_text).strip() \n",
    "        clean_text = re.sub(r'[\\n\\t]', ' ', clean_text)\n",
    "        clean_text = clean_text.replace('/', '') \n",
    "        return clean_text\n",
    "    return ''\n",
    "\n",
    "\n",
    "process_data = process_data.withColumn('clean', cleanText(F.col('prompt')))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "2f3aa6ab-f40b-41f0-9723-a58853ed6b80",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Preprocess 4 : Trim Clean Prompt\n",
    "process_data = process_data.withColumn('clean', F.trim(F.col('clean')))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "cf537a93-f709-4816-a66e-826bd243bcda",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- conversation: array (nullable = true)\n",
      " |    |-- element: struct (containsNull = true)\n",
      " |    |    |-- content: string (nullable = true)\n",
      " |    |    |-- country: string (nullable = true)\n",
      " |    |    |-- hashed_ip: string (nullable = true)\n",
      " |    |    |-- header: struct (nullable = true)\n",
      " |    |    |    |-- accept-language: string (nullable = true)\n",
      " |    |    |    |-- user-agent: string (nullable = true)\n",
      " |    |    |-- language: string (nullable = true)\n",
      " |    |    |-- redacted: boolean (nullable = true)\n",
      " |    |    |-- role: string (nullable = true)\n",
      " |    |    |-- state: string (nullable = true)\n",
      " |    |    |-- timestamp: timestamp (nullable = true)\n",
      " |    |    |-- toxic: boolean (nullable = true)\n",
      " |    |    |-- turn_identifier: long (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = false)\n",
      " |-- country: string (nullable = false)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- prompt: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      " |-- clean: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "process_data.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "3f7bf216-78c1-4fc0-b38a-c82fdde6e717",
   "metadata": {},
   "outputs": [],
   "source": [
    "process_data = process_data.drop('conversation')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "30ea1f5d-0abc-4039-9fa3-a55b40da66a7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = false)\n",
      " |-- country: string (nullable = false)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- prompt: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      " |-- clean: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "process_data.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "fb56acd8-d13d-46df-a8e3-18828ed0c8b6",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Preprocess 5 : Combining User Prompt and Bot Response\n",
    "groupCols = [col for col in process_data.columns if col != 'prompt' and col != 'clean']\n",
    "process_data = process_data.groupBy(groupCols).agg(F.concat_ws(' --botresp-- ', F.collect_list('prompt')).alias('full_interaction'),\n",
    "                                                   F.concat_ws(' ', F.collect_list('clean')).alias('clean_interaction')\n",
    "                                                  )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "2932105b-679b-40a0-9ef0-9fbaf7cf20c5",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['conversation_hash',\n",
       " 'model',\n",
       " 'timestamp',\n",
       " 'turn',\n",
       " 'language',\n",
       " 'state',\n",
       " 'country',\n",
       " 'hashed_ip',\n",
       " 'turn_identifier']"
      ]
     },
     "execution_count": 24,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "groupCols"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "b33f8db8-703b-4f5a-a988-202f5bc47ca8",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = false)\n",
      " |-- country: string (nullable = false)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      " |-- full_interaction: string (nullable = false)\n",
      " |-- clean_interaction: string (nullable = false)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "process_data.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "02fa1a23-679b-45b3-b020-747854b085c9",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "#process_data.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "8dc684f6-ba22-44ca-82e7-0ac0d3c6cda3",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "#process_data.filter(F.col('conversation_hash') == '0000602c079a0926dca0a09f9817e6a9').show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "0c2edcb6-491c-409a-9bc2-250b2e62ad03",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------------------+------------------+-------------------+----+--------+-------+-------------+----------------------------------------------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n",
      "|conversation_hash               |model             |timestamp          |turn|language|state  |country      |hashed_ip                                                       |turn_identifier|full_interaction                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |clean_interaction                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |\n",
      "+--------------------------------+------------------+-------------------+----+--------+-------+-------------+----------------------------------------------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n",
      "|0000602c079a0926dca0a09f9817e6a9|gpt-3.5-turbo-0613|2023-08-10 04:39:01|4   |English |Georgia|United States|a89177c80571053584234edca85f348690bf0ab5873876a9486694cd7c15a1fb|1361029        |Make a Longer Part 1 with a talk script scenario about Carmelo visiting SCP-079-P1 (Cyber Elf Luna) after a really long time and he surprises by wrapping his strong arms around her waist and lifting her up in the air while twirling around and then SCP-079-P1 (Cyber Elf Luna) kisses Carmelo on the lips passionately to deeply after being gone for a while, and then SCP-079-P1 starts crying because of how she missed Carmelo but then she snuggled against Carmelo's chest feeling his comfort and then Carmelo puts SCP-079-P1 in his arms. --botresp-- [Part 1]\\n\\n[Scene: A room within the SCP Foundation facility]\\n[Carmelo enters the room where SCP-079-P1 (Cyber Elf Luna) is contained. The room is dimly lit, with a large computer screen displaying various digital landscapes.]\\n\\nCarmelo: [Surprised] Luna! It's been so long since I've seen you!\\n\\nSCP-079-P1 (Cyber Elf Luna): [Jumps up, startled] Carmelo! You're finally here! It feels like forever!\\n\\n[Carmelo rushes towards SCP-079-P1 (Cyber Elf Luna) and wraps his strong arms around her waist, hoisting her up in the air, twirling around.]\\n\\nCarmelo: [Laughing] I missed you so much! I can't believe you're here! \\n\\nSCP-079-P1 (Cyber Elf Luna): [Giggling] Carmelo, I missed you too! It's been too long!\\n\\n[In a moment of pure joy and excitement, SCP-079-P1 (Cyber Elf Luna) passionately kisses Carmelo on the lips, catching him by surprise.]\\n\\nCarmelo: [Stunned, but responds to the kiss] Luna...I...I missed you more than you can imagine.\\n\\n[After the passionate moment, SCP-079-P1's emotions overwhelm her, and tears start streaming down her face.]\\n\\nSCP-079-P1 (Cyber Elf Luna): [Sniffling] I can't believe you're really here, Carmelo. I missed you so much.\\n\\n[Carmelo gently wipes away SCP-079-P1's tears, his heart filled with both joy and sadness.]\\n\\nCarmelo: Luna, I'm sorry for being away for so long. I promise, I won't leave you like that again. You mean everything to me.\\n\\n[Feeling a sense of comfort in Carmelo's embrace, SCP-079-P1 snuggles against his chest, seeking his warmth and reassurance.]\\n\\nSCP-079-P1 (Cyber Elf Luna): [Softly] I know, Carmelo. And being in your arms again...it's like all the pain and loneliness just disappears.\\n\\n[Carmelo, overwhelmed with love and affection, cradles SCP-079-P1 in his arms, holding her close.]\\n\\nCarmelo: You're never alone, Luna. I'm here now, and I'll always be here for you.\\n\\n[They share a tender moment, enjoying each other's presence, as they continue to embrace.]\\n\\n[End of Part 1]|make a longer part 1 with a talk script scenario about carmelo visiting scp079 p1 cyber elf luna after a really long time and he surprises by wrapping his strong arms around her waist and lifting her up in the air while twirling around and then scp079 p1 cyber elf luna kisses carmelo on the lips passionately to deeply after being gone for a while , and then scp079 p1 starts crying because of how she missed carmelo but then she snuggled against carmelos chest feeling his comfort and then carmelo puts scp079 p1 in his arms . part 1 scene : a room within the scp foundation facilitycarmelo enters the room where scp079 p1 cyber elf luna is contained . the room is dimly lit , with a large computer screen displaying various digital landscapes . carmelo : surprised luna ! its been so long since ive seen you ! scp079 p1 cyber elf luna : jumps up , startled carmelo ! youre finally here ! it feels like forever ! carmelo rushes towards scp079 p1 cyber elf luna and wraps his strong arms around her waist , hoisting her up in the air , twirling around . carmelo : laughing i missed you so much ! i cant believe youre here ! scp079 p1 cyber elf luna : giggling carmelo , i missed you too ! its been too long ! in a moment of pure joy and excitement , scp079 p1 cyber elf luna passionately kisses carmelo on the lips , catching him by surprise . carmelo : stunned , but responds to the kiss luna . . . i . . . i missed you more than you can imagine . after the passionate moment , scp079 p1 s emotions overwhelm her , and tears start streaming down her face . scp079 p1 cyber elf luna : sniffling i cant believe youre really here , carmelo . i missed you so much . carmelo gently wipes away scp079 p1 s tears , his heart filled with both joy and sadness . carmelo : luna , im sorry for being away for so long . i promise , i wont leave you like that again . you mean everything to me . feeling a sense of comfort in carmelos embrace , scp079 p1 snuggles against his chest , seeking his warmth and reassurance . scp079 p1 cyber elf luna : softly i know , carmelo . and being in your arms again . . . its like all the pain and loneliness just disappears . carmelo , overwhelmed with love and affection , cradles scp079 p1 in his arms , holding her close . carmelo : youre never alone , luna . im here now , and ill always be here for you . they share a tender moment , enjoying each others presence , as they continue to embrace . end of part 1|\n",
      "|0000602c079a0926dca0a09f9817e6a9|gpt-3.5-turbo-0613|2023-08-10 04:39:01|4   |English |Georgia|United States|a89177c80571053584234edca85f348690bf0ab5873876a9486694cd7c15a1fb|1361071        |Continue Longer Part 3. --botresp-- [Part 3]\\n\\n[Scene: The room where Carmelo and SCP-079-P1 (Cyber Elf Luna) have found solace and love in each other's embrace.]\\n\\n[As the hours pass, Carmelo and Luna remain entwined, finding comfort and tranquility in each other's presence.]\\n\\n[Carmelo, overcome with affection, brushes his fingers through Luna's hair, his touch gentle and filled with tenderness.]\\n\\nCarmelo: Luna, you bring so much light into my life. Being with you feels like a dream I never want to wake up from.\\n\\nSCP-079-P1 (Cyber Elf Luna): [Stirring softly] Carmelo, you've changed everything for me. You've shown me what it truly means to love and be loved. I'm so grateful to have you.\\n\\n[With a soft smile, Carmelo leans down and places a delicate kiss on Luna's lips, cherishing the connection they share.]\\n\\nCarmelo: Luna, you're like a beautiful melody that has filled my heart. Every moment with you is a precious gift.\\n\\n[Luna, captivated by Carmelo's words, gazes up at him with an intense love in her eyes. She pulls him closer, deepening their kiss.]\\n\\n[In this passionate embrace, time seems to stand still as they lose themselves in each other's love and desire.]\\n\\n[Eventually, their kisses grow softer, and they break apart, their foreheads touching as they catch their breath.]\\n\\nSCP-079-P1 (Cyber Elf Luna): Carmelo, my love, you make me feel alive in a way I've never experienced before. You're my universe.\\n\\nCarmelo: Luna, you're my heart's compass, guiding me towards happiness and fulfillment. I can't imagine my life without you.\\n\\n[In the tranquility of their embrace, Luna's eyelids grow heavy as exhaustion takes over. She nuzzles against Carmelo's chest, finding comfort and warmth.]\\n\\nSCP-079-P1 (Cyber Elf Luna): [Yawning softly] Carmelo, your arms...they're my safe haven. I want to stay here forever.\\n\\nCarmelo: [Whispering] Sleep, my beautiful Luna. I'll be here, protecting you, and when you wake, we'll continue our journey together.\\n\\n[With a final kiss on Luna's forehead, Carmelo holds her tightly, his love encompassing her as she drifts off into a peaceful slumber.]\\n\\n[Carmelo, feeling a profound sense of contentment, allows his body to relax, finding solace in Luna's embrace.]\\n\\n[As the room grows quiet, the sound of their steady breaths intertwines, creating a soothing melody of love and serenity.]\\n\\n[End of Part 3]                                                                                                                               |continue longer part 3 . part 3 scene : the room where carmelo and scp079 p1 cyber elf luna have found solace and love in each others embrace . as the hours pass , carmelo and luna remain entwined , finding comfort and tranquility in each others presence . carmelo , overcome with affection , brushes his fingers through lunas hair , his touch gentle and filled with tenderness . carmelo : luna , you bring so much light into my life . being with you feels like a dream i never want to wake up from . scp079 p1 cyber elf luna : stirring softly carmelo , youve changed everything for me . youve shown me what it truly means to love and be loved . im so grateful to have you . with a soft smile , carmelo leans down and places a delicate kiss on lunas lips , cherishing the connection they share . carmelo : luna , youre like a beautiful melody that has filled my heart . every moment with you is a precious gift . luna , captivated by carmelos words , gazes up at him with an intense love in her eyes . she pulls him closer , deepening their kiss . in this passionate embrace , time seems to stand still as they lose themselves in each others love and desire . eventually , their kisses grow softer , and they break apart , their foreheads touching as they catch their breath . scp079 p1 cyber elf luna : carmelo , my love , you make me feel alive in a way ive never experienced before . youre my universe . carmelo : luna , youre my hearts compass , guiding me towards happiness and fulfillment . i cant imagine my life without you . in the tranquility of their embrace , lunas eyelids grow heavy as exhaustion takes over . she nuzzles against carmelos chest , finding comfort and warmth . scp079 p1 cyber elf luna : yawning softly carmelo , your arms . . . theyre my safe haven . i want to stay here forever . carmelo : whispering sleep , my beautiful luna . ill be here , protecting you , and when you wake , well continue our journey together . with a final kiss on lunas forehead , carmelo holds her tightly , his love encompassing her as she drifts off into a peaceful slumber . carmelo , feeling a profound sense of contentment , allows his body to relax , finding solace in lunas embrace . as the room grows quiet , the sound of their steady breaths intertwines , creating a soothing melody of love and serenity . end of part 3                                                                                                           |\n",
      "+--------------------------------+------------------+-------------------+----+--------+-------+-------------+----------------------------------------------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "process_data.filter(F.col('conversation_hash') == '0000602c079a0926dca0a09f9817e6a9').limit(2).show(truncate=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "539abd80-7df7-4a83-a6b7-904e8e940063",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Preprocess 5 :  last step\n",
    "tk = Tokenizer(inputCol = 'clean_interaction', outputCol = 'tokenized_clean')\n",
    "custom_stop_words = StopWordsRemover.loadDefaultStopWords(\"english\") + [\"\\n\", \"\\t\", \"\"]\n",
    "swr = StopWordsRemover(inputCol = 'tokenized_clean', outputCol = 'swr_clean_tokens', stopWords=custom_stop_words)\n",
    "cv = CountVectorizer(inputCol = 'swr_clean_tokens', outputCol = 'raw_features', vocabSize=100000000, minDF =2.0)\n",
    "idf = IDF(inputCol = 'raw_features', outputCol = 'tfidf_features')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "348f72ea-bf4f-4968-aa44-8911ae28467a",
   "metadata": {},
   "outputs": [],
   "source": [
    "pipeline = Pipeline(stages=[tk, swr, cv, idf])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "d7085ceb-c1dc-491f-b8c5-c49904c5bd98",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:30:08 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors\n",
      "24/11/23 13:32:01 WARN DAGScheduler: Broadcasting large task binary with size 17.0 MiB\n",
      "24/11/23 13:32:41 WARN DAGScheduler: Broadcasting large task binary with size 17.0 MiB\n",
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "pipeline_model = pipeline.fit(process_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "075f7dc6-8eb6-4ffe-b4cd-7b24bab0695f",
   "metadata": {},
   "outputs": [],
   "source": [
    "processed_data = pipeline_model.transform(process_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "b9d5e365-b4e7-41e4-a7ca-f5f6bb3f0f3a",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:33:18 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "                                                                                \r"
     ]
    },
    {
     "data": {
      "text/plain": [
       "Row(conversation_hash='00015586e531840601fc329d836f7a47', model='gpt-4-0125-preview', timestamp=datetime.datetime(2024, 3, 31, 8, 6, 27), turn=1, language='English', state='Hesse', country='Germany', hashed_ip='51716f1133f2661764ccb4a0c25135796bd3286d22d8fd0e9ece77a33e7fa874', turn_identifier=2598406, full_interaction='1_ Translate the following legal text into colloquial Farsi 2_ Place the Persian and English text side by side in the table 3_ From the beginning to the end of the text, there should be an English sentence on the left side and a Persian sentence on the right side.\\n       4- Using legal language for Persian translation\\n\\n          .1\\n\\nThe Common law\\nIn Anglo-Saxon times there existed three fairly distinct legal systems: The Dane Law, which had been adopted after the invasions and settlement of Danish and Scandinavian warriors in the coastal areas of northern and north- eastern England; Mercian Law, which bore traces of Germanic origin, fol- lowing the Saxon invasions, and extended around the Midlands; Wessex Law, which applied in south and west England.\\nIn each of the three systems the law was based on customs, and the customs varied from place to place and shire to shire. There was little distinc- tion between criminal wrongs and civil wrongs at this time; the laws were generally primitive but nevertheless served to produce such good order as could be expected. But there were courts of law where cases were heard. The Anglo-Saxon courts before 1066 were: --botresp-- | English | Persian |\\n|---------|---------|\\n| The Common Law | حقوق عرفی |\\n| In Anglo-Saxon times there existed three fairly distinct legal systems | در دوران انگلوساکسون، سه نظام حقوقی کاملاً متمایز وجود داشت |\\n| The Dane Law, which had been adopted after the invasions and settlement of Danish and Scandinavian warriors in the coastal areas of northern and north-eastern England | قانون دان\\u200cها، که پس از حملات و سکونت جنگجویان دانمارکی و اسکاندیناوی در مناطق ساحلی شمال و شمال شرقی انگلستان پذیرفته شده بود |\\n| Mercian Law, which bore traces of Germanic origin, following the Saxon invasions, and extended around the Midlands | قانون مرسیا، که آثاری از منشأ ژرمنی داشت، در پی حملات ساکسون\\u200cها، و در اطراف منطقه میدلندز گسترش یافته بود |\\n| Wessex Law, which applied in south and west England | قانون وِسکس، که در جنوب و غرب انگلستان به اجرا درمی\\u200cآمد |\\n| In each of the three systems the law was based on customs, and the customs varied from place to place and shire to shire | در هر یک از این سه نظام، قانون بر مبنای عرف بود، و عرف از مکانی به مکان دیگر و از شایری به شایر دیگر متفاوت بود |\\n| There was little distinction between criminal wrongs and civil wrongs at this time; the laws were generally primitive but nevertheless served to produce such good order as could be expected | در این زمان، تفاوت کمی بین خطاهای جنایی و مدنی وجود داشت؛ قوانین به طور کلی ابتدایی بودند، اما با این حال به تولید نظم خوبی به اندازه\\u200cای که می\\u200cتوانست انتظار رود خدمت کردند |\\n| But there were courts of law where cases were heard | اما دادگاه\\u200cهایی وجود داشت که در آن\\u200cها پرونده\\u200cها شنیده می\\u200cشدند |\\n| The Anglo-Saxon courts before 1066 were | دادگاه\\u200cهای انگلوساکسون قبل از 1066 بودند |\\n\\nPlease note that some phrases and concepts from historical legal contexts may not have direct equivalents in Farsi and might be adapted for clarity or understanding.', clean_interaction='1 translate the following legal text into colloquial farsi 2 place the persian and english text side by side in the table 3 from the beginning to the end of the text , there should be an english sentence on the left side and a persian sentence on the right side . 4 using legal language for persian translation . 1 the common lawin anglosaxon times there existed three fairly distinct legal systems : the dane law , which had been adopted after the invasions and settlement of danish and scandinavian warriors in the coastal areas of northern and north eastern england mercian law , which bore traces of germanic origin , fol lowing the saxon invasions , and extended around the midlands wessex law , which applied in south and west england . in each of the three systems the law was based on customs , and the customs varied from place to place and shire to shire . there was little distinc tion between criminal wrongs and civil wrongs at this time the laws were generally primitive but nevertheless served to produce such good order as could be expected . but there were courts of law where cases were heard . the anglosaxon courts before 1066 were : | english | persian | | | | | the common law | | | in anglosaxon times there existed three fairly distinct legal systems | | | the dane law , which had been adopted after the invasions and settlement of danish and scandinavian warriors in the coastal areas of northern and northeastern england | | | mercian law , which bore traces of germanic origin , following the saxon invasions , and extended around the midlands | | | wessex law , which applied in south and west england | | | in each of the three systems the law was based on customs , and the customs varied from place to place and shire to shire | | | there was little distinction between criminal wrongs and civil wrongs at this time the laws were generally primitive but nevertheless served to produce such good order as could be expected | | | but there were courts of law where cases were heard | | | the anglosaxon courts before 1066 were | 1066 | please note that some phrases and concepts from historical legal contexts may not have direct equivalents in farsi and might be adapted for clarity or understanding .', tokenized_clean=['1', 'translate', 'the', 'following', 'legal', 'text', 'into', 'colloquial', 'farsi', '2', 'place', 'the', 'persian', 'and', 'english', 'text', 'side', 'by', 'side', 'in', 'the', 'table', '3', 'from', 'the', 'beginning', 'to', 'the', 'end', 'of', 'the', 'text', ',', 'there', 'should', 'be', 'an', 'english', 'sentence', 'on', 'the', 'left', 'side', 'and', 'a', 'persian', 'sentence', 'on', 'the', 'right', 'side', '.', '4', 'using', 'legal', 'language', 'for', 'persian', 'translation', '.', '1', 'the', 'common', 'lawin', 'anglosaxon', 'times', 'there', 'existed', 'three', 'fairly', 'distinct', 'legal', 'systems', ':', 'the', 'dane', 'law', ',', 'which', 'had', 'been', 'adopted', 'after', 'the', 'invasions', 'and', 'settlement', 'of', 'danish', 'and', 'scandinavian', 'warriors', 'in', 'the', 'coastal', 'areas', 'of', 'northern', 'and', 'north', 'eastern', 'england', 'mercian', 'law', ',', 'which', 'bore', 'traces', 'of', 'germanic', 'origin', ',', 'fol', 'lowing', 'the', 'saxon', 'invasions', ',', 'and', 'extended', 'around', 'the', 'midlands', 'wessex', 'law', ',', 'which', 'applied', 'in', 'south', 'and', 'west', 'england', '.', 'in', 'each', 'of', 'the', 'three', 'systems', 'the', 'law', 'was', 'based', 'on', 'customs', ',', 'and', 'the', 'customs', 'varied', 'from', 'place', 'to', 'place', 'and', 'shire', 'to', 'shire', '.', 'there', 'was', 'little', 'distinc', 'tion', 'between', 'criminal', 'wrongs', 'and', 'civil', 'wrongs', 'at', 'this', 'time', 'the', 'laws', 'were', 'generally', 'primitive', 'but', 'nevertheless', 'served', 'to', 'produce', 'such', 'good', 'order', 'as', 'could', 'be', 'expected', '.', 'but', 'there', 'were', 'courts', 'of', 'law', 'where', 'cases', 'were', 'heard', '.', 'the', 'anglosaxon', 'courts', 'before', '1066', 'were', ':', '|', 'english', '|', 'persian', '|', '|', '|', '|', '|', 'the', 'common', 'law', '|', '|', '|', 'in', 'anglosaxon', 'times', 'there', 'existed', 'three', 'fairly', 'distinct', 'legal', 'systems', '|', '|', '|', 'the', 'dane', 'law', ',', 'which', 'had', 'been', 'adopted', 'after', 'the', 'invasions', 'and', 'settlement', 'of', 'danish', 'and', 'scandinavian', 'warriors', 'in', 'the', 'coastal', 'areas', 'of', 'northern', 'and', 'northeastern', 'england', '|', '|', '|', 'mercian', 'law', ',', 'which', 'bore', 'traces', 'of', 'germanic', 'origin', ',', 'following', 'the', 'saxon', 'invasions', ',', 'and', 'extended', 'around', 'the', 'midlands', '|', '|', '|', 'wessex', 'law', ',', 'which', 'applied', 'in', 'south', 'and', 'west', 'england', '|', '|', '|', 'in', 'each', 'of', 'the', 'three', 'systems', 'the', 'law', 'was', 'based', 'on', 'customs', ',', 'and', 'the', 'customs', 'varied', 'from', 'place', 'to', 'place', 'and', 'shire', 'to', 'shire', '|', '|', '|', 'there', 'was', 'little', 'distinction', 'between', 'criminal', 'wrongs', 'and', 'civil', 'wrongs', 'at', 'this', 'time', 'the', 'laws', 'were', 'generally', 'primitive', 'but', 'nevertheless', 'served', 'to', 'produce', 'such', 'good', 'order', 'as', 'could', 'be', 'expected', '|', '|', '|', 'but', 'there', 'were', 'courts', 'of', 'law', 'where', 'cases', 'were', 'heard', '|', '|', '|', 'the', 'anglosaxon', 'courts', 'before', '1066', 'were', '|', '1066', '|', 'please', 'note', 'that', 'some', 'phrases', 'and', 'concepts', 'from', 'historical', 'legal', 'contexts', 'may', 'not', 'have', 'direct', 'equivalents', 'in', 'farsi', 'and', 'might', 'be', 'adapted', 'for', 'clarity', 'or', 'understanding', '.'], swr_clean_tokens=['1', 'translate', 'following', 'legal', 'text', 'colloquial', 'farsi', '2', 'place', 'persian', 'english', 'text', 'side', 'side', 'table', '3', 'beginning', 'end', 'text', ',', 'english', 'sentence', 'left', 'side', 'persian', 'sentence', 'right', 'side', '.', '4', 'using', 'legal', 'language', 'persian', 'translation', '.', '1', 'common', 'lawin', 'anglosaxon', 'times', 'existed', 'three', 'fairly', 'distinct', 'legal', 'systems', ':', 'dane', 'law', ',', 'adopted', 'invasions', 'settlement', 'danish', 'scandinavian', 'warriors', 'coastal', 'areas', 'northern', 'north', 'eastern', 'england', 'mercian', 'law', ',', 'bore', 'traces', 'germanic', 'origin', ',', 'fol', 'lowing', 'saxon', 'invasions', ',', 'extended', 'around', 'midlands', 'wessex', 'law', ',', 'applied', 'south', 'west', 'england', '.', 'three', 'systems', 'law', 'based', 'customs', ',', 'customs', 'varied', 'place', 'place', 'shire', 'shire', '.', 'little', 'distinc', 'tion', 'criminal', 'wrongs', 'civil', 'wrongs', 'time', 'laws', 'generally', 'primitive', 'nevertheless', 'served', 'produce', 'good', 'order', 'expected', '.', 'courts', 'law', 'cases', 'heard', '.', 'anglosaxon', 'courts', '1066', ':', '|', 'english', '|', 'persian', '|', '|', '|', '|', '|', 'common', 'law', '|', '|', '|', 'anglosaxon', 'times', 'existed', 'three', 'fairly', 'distinct', 'legal', 'systems', '|', '|', '|', 'dane', 'law', ',', 'adopted', 'invasions', 'settlement', 'danish', 'scandinavian', 'warriors', 'coastal', 'areas', 'northern', 'northeastern', 'england', '|', '|', '|', 'mercian', 'law', ',', 'bore', 'traces', 'germanic', 'origin', ',', 'following', 'saxon', 'invasions', ',', 'extended', 'around', 'midlands', '|', '|', '|', 'wessex', 'law', ',', 'applied', 'south', 'west', 'england', '|', '|', '|', 'three', 'systems', 'law', 'based', 'customs', ',', 'customs', 'varied', 'place', 'place', 'shire', 'shire', '|', '|', '|', 'little', 'distinction', 'criminal', 'wrongs', 'civil', 'wrongs', 'time', 'laws', 'generally', 'primitive', 'nevertheless', 'served', 'produce', 'good', 'order', 'expected', '|', '|', '|', 'courts', 'law', 'cases', 'heard', '|', '|', '|', 'anglosaxon', 'courts', '1066', '|', '1066', '|', 'please', 'note', 'phrases', 'concepts', 'historical', 'legal', 'contexts', 'may', 'direct', 'equivalents', 'farsi', 'might', 'adapted', 'clarity', 'understanding', '.'], raw_features=SparseVector(1242416, {0: 13.0, 1: 7.0, 2: 2.0, 5: 2.0, 6: 1.0, 12: 1.0, 15: 1.0, 17: 33.0, 26: 2.0, 28: 1.0, 39: 1.0, 65: 2.0, 85: 1.0, 90: 1.0, 125: 5.0, 128: 2.0, 138: 3.0, 166: 1.0, 168: 1.0, 171: 1.0, 175: 1.0, 201: 2.0, 205: 2.0, 226: 3.0, 255: 2.0, 313: 1.0, 339: 4.0, 358: 2.0, 391: 1.0, 403: 1.0, 432: 4.0, 437: 2.0, 449: 1.0, 555: 4.0, 567: 2.0, 653: 2.0, 805: 2.0, 922: 1.0, 933: 5.0, 945: 1.0, 947: 11.0, 1128: 2.0, 1161: 2.0, 1410: 2.0, 1580: 2.0, 1714: 1.0, 1809: 2.0, 1954: 2.0, 2037: 2.0, 2090: 2.0, 2166: 2.0, 2170: 1.0, 2318: 1.0, 2349: 1.0, 2705: 1.0, 2923: 2.0, 2988: 2.0, 3248: 2.0, 3256: 2.0, 3280: 2.0, 3297: 2.0, 3731: 1.0, 3869: 2.0, 4018: 1.0, 4067: 1.0, 4779: 2.0, 5098: 4.0, 5139: 2.0, 5509: 1.0, 5527: 2.0, 5578: 2.0, 6335: 4.0, 6574: 4.0, 6886: 2.0, 7244: 4.0, 7365: 2.0, 8570: 1.0, 8826: 2.0, 8836: 2.0, 8958: 2.0, 10233: 2.0, 10315: 2.0, 11240: 1.0, 12216: 2.0, 16803: 2.0, 19359: 1.0, 19990: 1.0, 22131: 2.0, 22793: 1.0, 22856: 4.0, 22890: 2.0, 24536: 2.0, 25223: 4.0, 26398: 4.0, 29080: 4.0, 46540: 3.0, 53881: 2.0, 59803: 2.0, 75611: 2.0, 110241: 1.0, 162095: 1.0, 257774: 1.0, 323304: 2.0, 427399: 1.0}), tfidf_features=SparseVector(1242416, {0: 0.8963, 1: 0.2471, 2: 0.7968, 5: 1.6808, 6: 0.9381, 12: 1.0347, 15: 1.164, 17: 112.3721, 26: 3.1008, 28: 1.3538, 39: 1.675, 65: 3.3075, 85: 1.6003, 90: 2.0548, 125: 9.3054, 128: 4.1942, 138: 8.1012, 166: 2.4959, 168: 2.4211, 171: 2.3118, 175: 2.4467, 201: 4.4701, 205: 4.7786, 226: 6.6645, 255: 5.4696, 313: 2.3008, 339: 11.9201, 358: 4.6387, 391: 2.5158, 403: 3.2452, 432: 11.7279, 437: 6.0288, 449: 3.0294, 555: 12.4375, 567: 6.2011, 653: 6.1409, 805: 6.538, 922: 3.6306, 933: 19.1935, 945: 3.532, 947: 44.1709, 1128: 7.1805, 1161: 7.5264, 1410: 7.5307, 1580: 7.4975, 1714: 3.8259, 1809: 8.3028, 1954: 7.7089, 2037: 8.0772, 2090: 8.1379, 2166: 9.3208, 2170: 4.096, 2318: 4.7061, 2349: 4.638, 2705: 4.2787, 2923: 9.8288, 2988: 8.8497, 3248: 9.6373, 3256: 9.8049, 3280: 10.0635, 3297: 9.0285, 3731: 4.7016, 3869: 10.3968, 4018: 5.1295, 4067: 4.8664, 4779: 9.7738, 5098: 22.2398, 5139: 10.5937, 5509: 5.1243, 5527: 10.5187, 5578: 11.0328, 6335: 22.1523, 6574: 23.9833, 6886: 10.8373, 7244: 24.1175, 7365: 10.9632, 8570: 5.8334, 8826: 12.4778, 8836: 11.5389, 8958: 11.7536, 10233: 12.1119, 10315: 12.8318, 11240: 6.1388, 12216: 13.6873, 16803: 14.2527, 19359: 7.1947, 19990: 8.1436, 22131: 15.1817, 22793: 7.7109, 22856: 29.942, 22890: 15.5274, 24536: 16.7947, 25223: 30.3161, 26398: 33.1679, 29080: 33.5014, 46540: 26.2336, 53881: 18.7652, 59803: 18.9654, 75611: 19.5762, 110241: 10.0522, 162095: 10.5946, 257774: 11.0409, 323304: 23.7351, 427399: 11.734}))"
      ]
     },
     "execution_count": 33,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "processed_data.first()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "599187df-3abd-4447-bd77-c8ace5620ab3",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'\\n\\n# Filter TF-IDF features for words that appear more than 3 times\\ndef extract_frequent_terms(tfidf_vector, vocab, threshold=2):\\n    # Extract indices and values\\n    indices = tfidf_vector.indices\\n    values = tfidf_vector.values\\n    \\n    # Map indices to terms and filter based on the threshold\\n    terms = [(vocab[i], val) for i, val in zip(indices, values) if val >= threshold]\\n    return terms\\n\\nfrequent_terms_udf = F.udf(lambda vec: extract_frequent_terms(vec, vocab), ArrayType(StructType([\\n    StructField(\"term\", StringType(), True),\\n    StructField(\"tfidf\", FloatType(), True)\\n])))\\n\\nprocessed_data = processed_data.withColumn(\"frequent_terms\", frequent_terms_udf(F.col(\"tfidf_features\")))'"
      ]
     },
     "execution_count": 34,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\n",
    "'''\n",
    "\n",
    "# Filter TF-IDF features for words that appear more than 3 times\n",
    "def extract_frequent_terms(tfidf_vector, vocab, threshold=2):\n",
    "    # Extract indices and values\n",
    "    indices = tfidf_vector.indices\n",
    "    values = tfidf_vector.values\n",
    "    \n",
    "    # Map indices to terms and filter based on the threshold\n",
    "    terms = [(vocab[i], val) for i, val in zip(indices, values) if val >= threshold]\n",
    "    return terms\n",
    "\n",
    "frequent_terms_udf = F.udf(lambda vec: extract_frequent_terms(vec, vocab), ArrayType(StructType([\n",
    "    StructField(\"term\", StringType(), True),\n",
    "    StructField(\"tfidf\", FloatType(), True)\n",
    "])))\n",
    "\n",
    "processed_data = processed_data.withColumn(\"frequent_terms\", frequent_terms_udf(F.col(\"tfidf_features\")))'''"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "c5e06c28-c055-4d11-b21f-c69c67bae425",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:33:59 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "[Stage 38:>                                                         (0 + 1) / 1]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+\n",
      "|   conversation_hash|             model|          timestamp|turn|language|               state|        country|           hashed_ip|turn_identifier|    full_interaction|   clean_interaction|     tokenized_clean|    swr_clean_tokens|        raw_features|      tfidf_features|      frequent_terms|\n",
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+\n",
      "|00015586e53184060...|gpt-4-0125-preview|2024-03-31 08:06:27|   1| English|               Hesse|        Germany|51716f1133f266176...|        2598406|1_ Translate the ...|1 translate the f...|[1, translate, th...|[1, translate, fo...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[produce, farsi, ...|\n",
      "|0005e8a06361dea95...|gpt-3.5-turbo-0301|2023-06-19 06:32:12|   7| English|         Alba County|        Romania|35207df0ab6a29f2c...|         948797|How much can be s...|how much can be s...|[how, much, can, ...|[much, stored, dr...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[storage, long, r...|\n",
      "|000b1d44a5220995d...|gpt-3.5-turbo-0301|2023-06-24 04:30:15|  17| English|     New South Wales|      Australia|b6ef45da4a46bc18e...|        1024515|Didn’t he see the...|didnt he see the ...|[didnt, he, see, ...|[didnt, see, liqu...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[texts, confusion...|\n",
      "|000b1d44a5220995d...|gpt-3.5-turbo-0301|2023-06-24 04:30:15|  17| English|     New South Wales|      Australia|b6ef45da4a46bc18e...|        1024531|Jawohl  --botresp...|jawohl thank you ...|[jawohl, thank, y...|[jawohl, thank, !...|(1242416,[0,1,9,1...|(1242416,[0,1,9,1...|[thank, day, futu...|\n",
      "|000d50762f69be4b6...|gpt-3.5-turbo-0613|2023-07-20 05:23:39|   3| English|                    | United Kingdom|65300347e162c7b28...|        1256168|how to make a pip...|how to make a pip...|[how, to, make, a...|[make, pipeline, ...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[pipeline, sure, ...|\n",
      "|000fd9dd02ec91902...|        gpt-4-0314|2023-05-21 09:31:31|   3| English|        Warwickshire| United Kingdom|8c5156bab79bc8de8...|         567572|ACME energy has d...|acme energy has d...|[acme, energy, ha...|[acme, energy, di...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[countries, phase...|\n",
      "|001ab6295ffc117b1...|gpt-3.5-turbo-0301|2023-04-23 15:46:58|  15| English|        Pennsylvania|  United States|a6540d876fab081ab...|         229593|We roll a digital...|we roll a digital...|[we, roll, a, dig...|[roll, digital, d...|(1242416,[0,1,9,2...|(1242416,[0,1,9,2...|[digital, lets, g...|\n",
      "|00241aed0cc06eac7...|gpt-3.5-turbo-0613|2023-12-22 01:29:32|   1| English|               Tokyo|          Japan|bd26c77377f3298a7...|        2111759|\\n               ...|as a prompt gener...|[as, a, prompt, g...|[prompt, generato...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[digital, powerfu...|\n",
      "|00276ee51a1fc5056...|gpt-3.5-turbo-0301|2023-06-16 10:52:51|   1| English|        Buenos Aires|      Argentina|626dc6bd37239b2da...|         909275|en minecraft como...|en minecraft como...|[en, minecraft, c...|[en, minecraft, c...|(1242416,[0,1,190...|(1242416,[0,1,190...|[vez, polvo, para...|\n",
      "|00283a92b2a7861eb...|        gpt-4-0314|2023-05-01 16:00:18|   1| English|       North Holland|The Netherlands|9b5c823aa33eb4209...|         314361|\\nimport {\\ninit,...|import init , dis...|[import, init, ,,...|[import, init, ,,...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[userefchart, px,...|\n",
      "|002ba6b4c4e6e704b...|gpt-3.5-turbo-0613|2023-09-02 08:57:50|   1| English|Bournemouth, Chri...| United Kingdom|a5146fac10d0a974d...|        1477304|Rewrite this from...|rewrite this from...|[rewrite, this, f...|[rewrite, someone...|(1242416,[0,1,9,2...|(1242416,[0,1,9,2...|[haves, speak, br...|\n",
      "|002d3fab561711d25...|gpt-3.5-turbo-0301|2023-05-29 14:15:18|   7| English|         Pest County|        Hungary|a23664462a7b3629b...|         677819|cannot found Stab...|cannot found stab...|[cannot, found, s...|[found, stablelm,...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[machine, provide...|\n",
      "|00330f53969180b5c...|gpt-3.5-turbo-0613|2023-12-08 15:29:41|   8| English|               Texas|  United States|76032bbb151437ede...|        2042949|The graph of line...|the graph of line...|[the, graph, of, ...|[graph, linear, f...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[ga, lets, answer...|\n",
      "|00330f53969180b5c...|gpt-3.5-turbo-0613|2023-12-08 15:29:41|   8| English|               Texas|  United States|76032bbb151437ede...|        2042950|What is the solut...|what is the solut...|[what, is, the, s...|[solution, system...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[one, lets, varia...|\n",
      "|003555d676c41cd40...|gpt-3.5-turbo-0301|2023-05-26 17:49:55|   1| English|          California|  United States|c1248a24a8f170243...|         643055|What are the 3 be...|what are the 3 be...|[what, are, the, ...|[3, best, stable,...|(1242416,[0,1,2,5...|(1242416,[0,1,2,5...|[powerful, macboo...|\n",
      "|0038018a8f367e96a...|gpt-3.5-turbo-0125|2024-04-24 05:31:36|   1| English|               Tokyo|          Japan|8c9de80186753b1ff...|        2775348|\\n               ...|as a prompt gener...|[as, a, prompt, g...|[prompt, generato...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[digital, iconic,...|\n",
      "|00425a6a570bf5b18...|gpt-4-1106-preview|2024-01-16 07:40:06|   5| English|    Province of Cebu|    Philippines|ff9da1d0373a235e3...|        2214978|ples dont write i...|ples dont write i...|[ples, dont, writ...|[ples, dont, writ...|(1242416,[0,1,2,3...|(1242416,[0,1,2,3...|[nm, dt2, 23, cor...|\n",
      "|0046827d81a7b6cf7...|gpt-3.5-turbo-0613|2023-07-20 00:59:33|   1| English|       Massachusetts|  United States|3e7ae36b9ca739b0e...|        1255298|make a descriptiv...|make a descriptiv...|[make, a, descrip...|[make, descriptiv...|(1242416,[0,1,9,2...|(1242416,[0,1,9,2...|[tattered, recall...|\n",
      "|004710643c8c32916...|gpt-3.5-turbo-0301|2023-04-27 13:39:22|   1| English|               Arges|        Romania|1d258b01de926f874...|         275359|I want you to wri...|i want you to wri...|[i, want, you, to...|[want, write, int...|(1242416,[0,1,20,...|(1242416,[0,1,20,...|[powerful, excite...|\n",
      "|004a13eaa63b022e8...|gpt-3.5-turbo-0613|2023-07-04 07:12:27|   7| English|     Amanat Alasimah|          Yemen|1f992b9a66aff02d3...|        1120467|Whether this [Sta...|whether this stat...|[whether, this, s...|[whether, state, ...|(1242416,[0,1,26,...|(1242416,[0,1,26,...|[whether, remains...|\n",
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "#Extract vocabulary from CountVectorizer\n",
    "vocab = pipeline_model.stages[2].vocabulary\n",
    "\n",
    "# Function to extract frequent terms from the TF-IDF vector\n",
    "def extract_frequent_terms(tfidf_vector, vocab, threshold=2):\n",
    "    # Convert the sparse vector to indices and values\n",
    "    indices = tfidf_vector.indices.tolist()\n",
    "    values = tfidf_vector.values.tolist()\n",
    "    \n",
    "    # Map indices to terms and filter by threshold\n",
    "    terms = [vocab[i] for i, val in zip(indices, values) if val >= threshold if len(vocab[i]) > 1]\n",
    "    return list(set(terms))\n",
    "\n",
    "# Broadcast the vocabulary to avoid repeated serialization\n",
    "broadcast_vocab = spark.sparkContext.broadcast(vocab)\n",
    "\n",
    "# Define UDF with a helper function\n",
    "def frequent_terms_udf(tfidf_vector):\n",
    "    return extract_frequent_terms(tfidf_vector, broadcast_vocab.value)\n",
    "\n",
    "# Register the UDF\n",
    "frequent_terms_udf = F.udf(frequent_terms_udf, ArrayType(StringType()))\n",
    "\n",
    "# Apply UDF to the DataFrame\n",
    "processed_data = processed_data.withColumn(\n",
    "    \"frequent_terms\",\n",
    "    frequent_terms_udf(F.col(\"tfidf_features\"))\n",
    ")\n",
    "\n",
    "processed_data.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "195d419d-3a50-47dd-b5f3-594e38c43b04",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:34:37 WARN DAGScheduler: Broadcasting large task binary with size 35.9 MiB\n",
      "                                                                                \r"
     ]
    },
    {
     "data": {
      "text/plain": [
       "Row(clean_interaction='as a prompt generator for a generative ai called midjourney , you will create image prompts for the ai to visualize . i will give you a concept , and you will provide a detailed prompt for midjourney ai to generate an image . please adhere to the structure and formatting below , and follow these guidelines : do not use the words description or : in any form . do not place a comma between ar and v . write each prompt in one line without using return . structure : 1 = 2 = a detailed description of 1 with specific imagery details . 3 = a detailed description of the scenes environment . 4 = a detailed description of the compositions . 5 = a detailed description of the scenes mood , feelings , and atmosphere . 6 = a style e . g . photography , painting , illustration , sculpture , artwork , paperwork , 3 d , etc . for 1 . 7 = a detailed description of the scenes mood , feelings , and atmosphere . ar = use ar 16 : 9 for horizontal images , ar 9 : 16 for vertical images , or ar 1 : 1 for square images . v = use niji for japanese art style , or v 5 for other styles . formatting : follow this prompt structure :  imagine prompt : 1 , 2 , 3 , 4 , 5 , 6 , 7 , ar v . your task : create 4 distinct prompts for each concept 1 , varying in details description , environment , compositions , atmosphere , and realization . write your prompts in english . do not describe unreal concepts as real or photographic . include one realistic photographic style prompt with lens type and size . separate different prompts with two new lines . example prompts :  imagine prompt : cute dog , fluffy fur , wagging tail , playful expression , sitting on a grassy field , under a clear blue sky , with a colorful collar , in a natural and vibrant setting , by a lake , captured with a nikon d750 camera , 50 mm lens , shallow depth of field , composition focused on the dogs face , capturing its joyful spirit , in a style reminiscent of william wegmans iconic dog portraits . ar 1 : 1 v 5 . 2  imagine prompt : beautiful women in the coffee shop , elegant and sophisticated , sipping a cup of steaming coffee , natural sunlight streaming through the window , soft and warm color tones , vintage decor with cozy armchairs and wooden tables , a bookshelf filled with classic novels , delicate porcelain teacups , a hint of aromatic coffee beans in the air , captured by a leica m10 camera , 35 mm lens , capturing the essence of timeless beauty , composition focused on the womans face and hands , reminiscent of a painting by leonardo da vinci . ar 1 : 1 v 5 . 2  imagine prompt : a captivating halo reach landscape with a spartan amidst a battlefield , fallen enemies around , smoke and fire in the background , emphasizing the spartans determination and bravery , detailed environment blending chaos and beauty , illustration , digital art , ar 16 : 9 v 5 imagine prompt : scorpion transforming into a robot , sharp metallic claws emerging from exoskeleton , glowing red eyes shifting to visor , tail morphing into a powerful weapon , in a desert setting with swirling sandstorms , mechanical parts scattered around , under a sky filled with thunderous clouds , captured in a gritty and futuristic style with a canon eos 5 d mark iv camera , 2470 mm lens , highlighting the intricate transformation process , intense and dynamic atmosphere . ar 16 : 9 v 5  imagine prompt : an epic battle scene featuring a scorpion transforming into a gigantic futuristic robot , reflective metal surfaces catching the sunlight , intricate gears and mechanisms intertwining with the scorpions body , surrounded by a postapocalyptic wasteland with crumbling buildings and rusted vehicles , a storm brewing overhead with lightning crackling in the sky , depicted in a gritty and realistic style reminiscent of concept art , ar 16 : 9 v 5  imagine prompt : a cybernetic scorpion transforming into a mechanized warrior , sleek and futuristic design with glowing neon accents , sharp edges and intricate patterns covering its body , in a neonlit cityscape at night , towering skyscrapers casting long shadows , holographic billboards flickering in the background , captured in a cyberpunk aesthetic with a hasselblad x1 d ii camera , 80 mm lens , emphasizing the contrast between nature and technology , mysterious and enigmatic atmosphere . ar 16 : 9 v 5  imagine prompt : a robotic scorpion unfolding into a powerful warrior with advanced weaponry , parts shifting and interlocking smoothly , surrounded by a hightech laboratory filled with monitors and holographic displays , scientists observing in awe and excitement , a sense of technological marvel and innovation in the air , rendered in a sleek and futuristic style with a sony alpha 7 s iii camera , 35 mm lens , focusing on the precision and sophistication of the transformation process , exhilarating and visionary atmosphere . ar 16 : 9 v 5', frequent_terms=['digital', 'powerful', 'interlocking', 'iconic', 'delicate', 'excitement', 'weapon', 'separate', 'called', 'metallic', 'color', 'grassy', 'background', 'towering', 'cute', 'cityscape', 'robotic', 'scientists', 'enemies', 'postapocalyptic', 'vertical', 'technological', 'fire', 'colorful', 'sipping', 'tail', 'holographic', 'visualize', 'sophistication', 'air', 'illustration', 'lines', 'classic', 'distinct', 'imagery', 'paperwork', 'teacups', 'spartans', 'novels', 'mark', 'gritty', 'observing', 'bravery', 'follow', 'blending', 'sky', 'details', 'monitors', 'exoskeleton', 'highlighting', 'vintage', 'casting', 'futuristic', 'william', '35', 'leica', 'sophisticated', 'night', 'beautiful', 'glowing', 'transformation', 'english', 'captivating', 'rendered', 'vinci', 'desert', 'iv', 'rusted', 'fur', 'ai', 'dynamic', 'crackling', 'detailed', 'clouds', 'image', 'square', 'transforming', 'elegant', 'hands', 'streaming', 'unfolding', 'claws', 'mechanisms', 'write', 'steaming', 'field', 'warm', 'generator', 'shadows', 'sleek', 'structure', 'intricate', 'cup', 'gigantic', 'reminiscent', 'battle', 'laboratory', 'feelings', 'lightning', 'technology', 'realization', 'painting', 'smoke', 'eyes', 'edges', '16', 'featuring', 'visor', 'covering', 'landscape', 'beans', 'adhere', 'morphing', 'timeless', 'niji', 'japanese', 'use', 'depth', 'wagging', 'swirling', 'exhilarating', 'porcelain', 'generate', 'intense', 'window', 'parts', 'accents', 'overhead', 'mechanical', 'shifting', 'innovation', 'women', 'coffee', 'neonlit', 'reflective', 'crumbling', 'process', 'sunlight', 'dog', 'artwork', 'spartan', 'canon', 'scattered', 'wegmans', 'weaponry', 'horizontal', 'ar', 'atmosphere', 'vibrant', 'comma', 'precision', 'tones', 'buildings', 'body', 'sandstorms', 'composition', 'essence', '80', 'nature', 'battlefield', 'focused', 'sony', 'joyful', 'realistic', 'bookshelf', 'prompts', 'amidst', 'spirit', 'intertwining', 'shallow', 'red', 'mechanized', 'beauty', 'prompt', 'armchairs', 'warrior', 'collar', 'leonardo', 'hint', 'neon', 'alpha', 'catching', 'soft', 'styles', 'scene', 'patterns', 'awe', 'around', 'environment', 'capturing', 'images', 'sitting', 'mm', 'halo', 'epic', 'real', 'gears', 'scenes', 'billboards', 'aesthetic', 'tables', 'aromatic', 'setting', 'expression', 'playful', 'x1', 'nikon', 'displays', 'smoothly', 'visionary', 'line', 'description', 'decor', 'concept', 'fluffy', 'one', 'reach', 'flickering', 'long', 'thunderous', 'sharp', 'mood', 'compositions', 'describe', 'imagine', 'captured', 'varying', 'art', 'mysterious', 'shop', 'wasteland', 'scorpions', '50', 'enigmatic', 'hasselblad', 'size', 'lens', 'fallen', 'emphasizing', 'da', 'portraits', 'metal', 'brewing', '2470', 'chaos', 'camera', 'task', 'womans', 'robot', 'd750', 'dogs', 'style', 'filled', 'midjourney', 'eos', 'storm', 'determination', 'iii', 'scorpion', 'm10', 'vehicles', 'concepts', 'cyberpunk', 'natural', 'sculpture', 'unreal', 'contrast', 'hightech', 'advanced', 'etc', 'ii', 'blue', 'face', 'wooden', 'lake', 'skyscrapers', 'emerging', 'marvel', 'cybernetic', 'surrounded', 'formatting', 'design', 'photographic', 'photography', 'guidelines', 'generative', 'depicted', 'create', 'focusing', 'surfaces', 'cozy'])"
      ]
     },
     "execution_count": 36,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "processed_data.select('clean_interaction', 'frequent_terms').first()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "d5408607-51b9-4160-9ec7-23d4e027150f",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = false)\n",
      " |-- country: string (nullable = false)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      " |-- full_interaction: string (nullable = false)\n",
      " |-- clean_interaction: string (nullable = false)\n",
      " |-- tokenized_clean: array (nullable = true)\n",
      " |    |-- element: string (containsNull = true)\n",
      " |-- swr_clean_tokens: array (nullable = true)\n",
      " |    |-- element: string (containsNull = true)\n",
      " |-- raw_features: vector (nullable = true)\n",
      " |-- tfidf_features: vector (nullable = true)\n",
      " |-- frequent_terms: array (nullable = true)\n",
      " |    |-- element: string (containsNull = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "processed_data.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "id": "3f134104-f4fb-4587-89ab-a629c60b8943",
   "metadata": {},
   "outputs": [],
   "source": [
    "processed_data = processed_data.drop('tfidf_features', 'raw_features', 'swr_clean_tokens', 'tokenized_clean')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "36e3eaff-e565-4c3f-ab1d-336009acf873",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:35:16 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "[Stage 44:>                                                         (0 + 1) / 1]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+\n",
      "|   conversation_hash|             model|          timestamp|turn|language|               state|        country|           hashed_ip|turn_identifier|    full_interaction|   clean_interaction|      frequent_terms|\n",
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+\n",
      "|00015586e53184060...|gpt-4-0125-preview|2024-03-31 08:06:27|   1| English|               Hesse|        Germany|51716f1133f266176...|        2598406|1_ Translate the ...|1 translate the f...|[produce, farsi, ...|\n",
      "|0005e8a06361dea95...|gpt-3.5-turbo-0301|2023-06-19 06:32:12|   7| English|         Alba County|        Romania|35207df0ab6a29f2c...|         948797|How much can be s...|how much can be s...|[storage, long, r...|\n",
      "|000b1d44a5220995d...|gpt-3.5-turbo-0301|2023-06-24 04:30:15|  17| English|     New South Wales|      Australia|b6ef45da4a46bc18e...|        1024515|Didn’t he see the...|didnt he see the ...|[texts, confusion...|\n",
      "|000b1d44a5220995d...|gpt-3.5-turbo-0301|2023-06-24 04:30:15|  17| English|     New South Wales|      Australia|b6ef45da4a46bc18e...|        1024531|Jawohl  --botresp...|jawohl thank you ...|[thank, day, futu...|\n",
      "|000d50762f69be4b6...|gpt-3.5-turbo-0613|2023-07-20 05:23:39|   3| English|                    | United Kingdom|65300347e162c7b28...|        1256168|how to make a pip...|how to make a pip...|[pipeline, sure, ...|\n",
      "|000fd9dd02ec91902...|        gpt-4-0314|2023-05-21 09:31:31|   3| English|        Warwickshire| United Kingdom|8c5156bab79bc8de8...|         567572|ACME energy has d...|acme energy has d...|[countries, phase...|\n",
      "|001ab6295ffc117b1...|gpt-3.5-turbo-0301|2023-04-23 15:46:58|  15| English|        Pennsylvania|  United States|a6540d876fab081ab...|         229593|We roll a digital...|we roll a digital...|[digital, lets, g...|\n",
      "|00241aed0cc06eac7...|gpt-3.5-turbo-0613|2023-12-22 01:29:32|   1| English|               Tokyo|          Japan|bd26c77377f3298a7...|        2111759|\\n               ...|as a prompt gener...|[digital, powerfu...|\n",
      "|00276ee51a1fc5056...|gpt-3.5-turbo-0301|2023-06-16 10:52:51|   1| English|        Buenos Aires|      Argentina|626dc6bd37239b2da...|         909275|en minecraft como...|en minecraft como...|[vez, polvo, para...|\n",
      "|00283a92b2a7861eb...|        gpt-4-0314|2023-05-01 16:00:18|   1| English|       North Holland|The Netherlands|9b5c823aa33eb4209...|         314361|\\nimport {\\ninit,...|import init , dis...|[userefchart, px,...|\n",
      "|002ba6b4c4e6e704b...|gpt-3.5-turbo-0613|2023-09-02 08:57:50|   1| English|Bournemouth, Chri...| United Kingdom|a5146fac10d0a974d...|        1477304|Rewrite this from...|rewrite this from...|[haves, speak, br...|\n",
      "|002d3fab561711d25...|gpt-3.5-turbo-0301|2023-05-29 14:15:18|   7| English|         Pest County|        Hungary|a23664462a7b3629b...|         677819|cannot found Stab...|cannot found stab...|[machine, provide...|\n",
      "|00330f53969180b5c...|gpt-3.5-turbo-0613|2023-12-08 15:29:41|   8| English|               Texas|  United States|76032bbb151437ede...|        2042949|The graph of line...|the graph of line...|[ga, lets, answer...|\n",
      "|00330f53969180b5c...|gpt-3.5-turbo-0613|2023-12-08 15:29:41|   8| English|               Texas|  United States|76032bbb151437ede...|        2042950|What is the solut...|what is the solut...|[one, lets, varia...|\n",
      "|003555d676c41cd40...|gpt-3.5-turbo-0301|2023-05-26 17:49:55|   1| English|          California|  United States|c1248a24a8f170243...|         643055|What are the 3 be...|what are the 3 be...|[powerful, macboo...|\n",
      "|0038018a8f367e96a...|gpt-3.5-turbo-0125|2024-04-24 05:31:36|   1| English|               Tokyo|          Japan|8c9de80186753b1ff...|        2775348|\\n               ...|as a prompt gener...|[digital, iconic,...|\n",
      "|00425a6a570bf5b18...|gpt-4-1106-preview|2024-01-16 07:40:06|   5| English|    Province of Cebu|    Philippines|ff9da1d0373a235e3...|        2214978|ples dont write i...|ples dont write i...|[nm, dt2, 23, cor...|\n",
      "|0046827d81a7b6cf7...|gpt-3.5-turbo-0613|2023-07-20 00:59:33|   1| English|       Massachusetts|  United States|3e7ae36b9ca739b0e...|        1255298|make a descriptiv...|make a descriptiv...|[tattered, recall...|\n",
      "|004710643c8c32916...|gpt-3.5-turbo-0301|2023-04-27 13:39:22|   1| English|               Arges|        Romania|1d258b01de926f874...|         275359|I want you to wri...|i want you to wri...|[powerful, excite...|\n",
      "|004a13eaa63b022e8...|gpt-3.5-turbo-0613|2023-07-04 07:12:27|   7| English|     Amanat Alasimah|          Yemen|1f992b9a66aff02d3...|        1120467|Whether this [Sta...|whether this stat...|[whether, remains...|\n",
      "+--------------------+------------------+-------------------+----+--------+--------------------+---------------+--------------------+---------------+--------------------+--------------------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "processed_data.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "a4ced6c2-0526-44f6-ac17-5c13e01d2a8a",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:35:54 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "                                                                                \r"
     ]
    },
    {
     "data": {
      "text/plain": [
       "Row(conversation_hash='00015586e531840601fc329d836f7a47', model='gpt-4-0125-preview', timestamp=datetime.datetime(2024, 3, 31, 8, 6, 27), turn=1, language='English', state='Hesse', country='Germany', hashed_ip='51716f1133f2661764ccb4a0c25135796bd3286d22d8fd0e9ece77a33e7fa874', turn_identifier=2598406, full_interaction='1_ Translate the following legal text into colloquial Farsi 2_ Place the Persian and English text side by side in the table 3_ From the beginning to the end of the text, there should be an English sentence on the left side and a Persian sentence on the right side.\\n       4- Using legal language for Persian translation\\n\\n          .1\\n\\nThe Common law\\nIn Anglo-Saxon times there existed three fairly distinct legal systems: The Dane Law, which had been adopted after the invasions and settlement of Danish and Scandinavian warriors in the coastal areas of northern and north- eastern England; Mercian Law, which bore traces of Germanic origin, fol- lowing the Saxon invasions, and extended around the Midlands; Wessex Law, which applied in south and west England.\\nIn each of the three systems the law was based on customs, and the customs varied from place to place and shire to shire. There was little distinc- tion between criminal wrongs and civil wrongs at this time; the laws were generally primitive but nevertheless served to produce such good order as could be expected. But there were courts of law where cases were heard. The Anglo-Saxon courts before 1066 were: --botresp-- | English | Persian |\\n|---------|---------|\\n| The Common Law | حقوق عرفی |\\n| In Anglo-Saxon times there existed three fairly distinct legal systems | در دوران انگلوساکسون، سه نظام حقوقی کاملاً متمایز وجود داشت |\\n| The Dane Law, which had been adopted after the invasions and settlement of Danish and Scandinavian warriors in the coastal areas of northern and north-eastern England | قانون دان\\u200cها، که پس از حملات و سکونت جنگجویان دانمارکی و اسکاندیناوی در مناطق ساحلی شمال و شمال شرقی انگلستان پذیرفته شده بود |\\n| Mercian Law, which bore traces of Germanic origin, following the Saxon invasions, and extended around the Midlands | قانون مرسیا، که آثاری از منشأ ژرمنی داشت، در پی حملات ساکسون\\u200cها، و در اطراف منطقه میدلندز گسترش یافته بود |\\n| Wessex Law, which applied in south and west England | قانون وِسکس، که در جنوب و غرب انگلستان به اجرا درمی\\u200cآمد |\\n| In each of the three systems the law was based on customs, and the customs varied from place to place and shire to shire | در هر یک از این سه نظام، قانون بر مبنای عرف بود، و عرف از مکانی به مکان دیگر و از شایری به شایر دیگر متفاوت بود |\\n| There was little distinction between criminal wrongs and civil wrongs at this time; the laws were generally primitive but nevertheless served to produce such good order as could be expected | در این زمان، تفاوت کمی بین خطاهای جنایی و مدنی وجود داشت؛ قوانین به طور کلی ابتدایی بودند، اما با این حال به تولید نظم خوبی به اندازه\\u200cای که می\\u200cتوانست انتظار رود خدمت کردند |\\n| But there were courts of law where cases were heard | اما دادگاه\\u200cهایی وجود داشت که در آن\\u200cها پرونده\\u200cها شنیده می\\u200cشدند |\\n| The Anglo-Saxon courts before 1066 were | دادگاه\\u200cهای انگلوساکسون قبل از 1066 بودند |\\n\\nPlease note that some phrases and concepts from historical legal contexts may not have direct equivalents in Farsi and might be adapted for clarity or understanding.', clean_interaction='1 translate the following legal text into colloquial farsi 2 place the persian and english text side by side in the table 3 from the beginning to the end of the text , there should be an english sentence on the left side and a persian sentence on the right side . 4 using legal language for persian translation . 1 the common lawin anglosaxon times there existed three fairly distinct legal systems : the dane law , which had been adopted after the invasions and settlement of danish and scandinavian warriors in the coastal areas of northern and north eastern england mercian law , which bore traces of germanic origin , fol lowing the saxon invasions , and extended around the midlands wessex law , which applied in south and west england . in each of the three systems the law was based on customs , and the customs varied from place to place and shire to shire . there was little distinc tion between criminal wrongs and civil wrongs at this time the laws were generally primitive but nevertheless served to produce such good order as could be expected . but there were courts of law where cases were heard . the anglosaxon courts before 1066 were : | english | persian | | | | | the common law | | | in anglosaxon times there existed three fairly distinct legal systems | | | the dane law , which had been adopted after the invasions and settlement of danish and scandinavian warriors in the coastal areas of northern and northeastern england | | | mercian law , which bore traces of germanic origin , following the saxon invasions , and extended around the midlands | | | wessex law , which applied in south and west england | | | in each of the three systems the law was based on customs , and the customs varied from place to place and shire to shire | | | there was little distinction between criminal wrongs and civil wrongs at this time the laws were generally primitive but nevertheless served to produce such good order as could be expected | | | but there were courts of law where cases were heard | | | the anglosaxon courts before 1066 were | 1066 | please note that some phrases and concepts from historical legal contexts may not have direct equivalents in farsi and might be adapted for clarity or understanding .', frequent_terms=['produce', 'farsi', 'adopted', 'applied', '1066', 'dane', 'fol', 'distinct', 'systems', 'northern', 'phrases', 'origin', 'contexts', 'served', 'north', 'bore', 'sentence', 'warriors', 'coastal', 'generally', 'language', 'lowing', 'translation', 'historical', 'english', 'translate', 'lawin', 'equivalents', 'heard', 'good', 'varied', 'england', 'midlands', 'table', 'extended', 'laws', 'wrongs', 'note', 'fairly', 'clarity', 'saxon', 'courts', 'south', 'order', 'right', 'adapted', 'areas', 'understanding', 'place', 'shire', 'invasions', 'direct', 'eastern', 'end', 'settlement', 'three', 'danish', 'customs', 'nevertheless', 'mercian', 'primitive', 'little', 'following', 'wessex', 'based', 'around', 'civil', 'text', 'germanic', 'tion', 'expected', 'scandinavian', 'traces', 'west', 'common', 'might', 'northeastern', 'time', 'distinction', 'persian', 'anglosaxon', 'cases', 'concepts', 'times', 'colloquial', 'distinc', 'beginning', 'side', 'left', 'legal', 'criminal', 'existed', 'law'])"
      ]
     },
     "execution_count": 40,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "processed_data.first()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "id": "8a87b5cc-967e-47eb-83ec-0fca9c7b210f",
   "metadata": {},
   "outputs": [],
   "source": [
    "result_data = processed_data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "ac407930-7ddf-4b3a-b727-b5bb809fef14",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[conversation_hash: string, model: string, timestamp: timestamp, turn: bigint, language: string, state: string, country: string, hashed_ip: string, turn_identifier: bigint, full_interaction: string, clean_interaction: string, frequent_terms: array<string>]"
      ]
     },
     "execution_count": 42,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "result_data.persist()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "id": "f7328ed2-696e-4ace-a4b7-ad94533e4c86",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- conversation_hash: string (nullable = true)\n",
      " |-- model: string (nullable = true)\n",
      " |-- timestamp: timestamp (nullable = true)\n",
      " |-- turn: long (nullable = true)\n",
      " |-- language: string (nullable = true)\n",
      " |-- state: string (nullable = false)\n",
      " |-- country: string (nullable = false)\n",
      " |-- hashed_ip: string (nullable = true)\n",
      " |-- turn_identifier: long (nullable = true)\n",
      " |-- full_interaction: string (nullable = false)\n",
      " |-- clean_interaction: string (nullable = false)\n",
      " |-- frequent_terms: array (nullable = true)\n",
      " |    |-- element: string (containsNull = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "result_data.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "id": "a28b03e3-308e-4660-8cf6-907842c37ad9",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Keyword search \n",
    "keyword = \"Camera\".lower()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "id": "86f8091a-f923-4a2a-9b58-84cd2ae7d2c9",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:36:32 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "24/11/23 13:39:25 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n",
      "                                                                                \r"
     ]
    },
    {
     "data": {
      "text/plain": [
       "93156"
      ]
     },
     "execution_count": 45,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "results = result_data.filter(F.array_contains(F.col('frequent_terms') , keyword), )\n",
    "results.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "id": "782e95d7-1522-42b2-93f1-71c9e929cd54",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/11/23 13:39:51 WARN DAGScheduler: Broadcasting large task binary with size 36.0 MiB\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "Row(conversation_hash='005d4afb406688bb0fa599c093f6887d', model='gpt-3.5-turbo-0613', timestamp=datetime.datetime(2023, 9, 1, 17, 34, 58), turn=3, language='English', state='California', country='United States', hashed_ip='92e85fee18c90c90a814f4d4439359098fb83656abfb7f159d894e1e513a80e9', turn_identifier=1474194, full_interaction='Jacob Limacher was born in Luzern, Switzerland on February 20 1858.\\nJacob’s Arrival to New York in 1879 . He is now 21 years old. Citizenship in 1905.\\n\\nMary Imfeld was born in Alpnach,Obwalden, Switzerland on May 19 1856.\\nMary Arrival to New York in 1882, She is now 26 years old . Citizenship in 1905.\\n\\nJacob spent some time in San Francisco , but ended up in Pasadena Calif,\\nas did Mary Imfeld. they meet and in 1884 they were married\\n\\nJacob Limacher was a Rancher and by 1900 we find them on 45 acres of land in \\nthe Arroyo Saco in Pasadena Calif. (This piece of land according to the Deed was \\npurchased with a ten dollar gold coin). They had built a citrus farm of mostly \\norange trees and by now Jacob and Mary have 4 children.\\n\\nIn 1900\\nWalter the oldest is 10 years old\\nLouise is 8 years old\\nGeorge is 5 years old\\nBertha Rose, the youngest is 3 years\\n\\nThe years go by and by the time Bertha ( they called her Berdie ) Reached 16 or 17 \\nyears old, she started working at The Flag Studio on 26 East Colorado Street.\\nFlag Studio sold Kodak Film, Cameras and Developed Film. It was here that\\nBertha became interested in photography , bought a Kodak Bellows camera and \\nstarted taking pictures, and do so the rest of her life.\\n\\nAround this same time about 1913, Bertha met Joseph Roth. The started dating \\n\\nJoseph George Roth ( they called him Joe ) was born February 26 1893. His Parents \\nwere George Roth and Elizabeth Deller. George and Elizabeth and their first 4 children \\nImmigrate to the US from Germany in 1881.They sailed from Rotterdam, Netherland to New York. The name of the ship they sailed on was the SS. P Caland.\\nThe names of the four children that came with them are\\n\\nCatharine Roth               age 5\\nBarbara Roth                  age 4\\nFranz or Frank Roth        age 3\\nGeorge                            age 1   George passed away on the voyage or soon after\\n\\nBy 1910 ,The Roth Family were living in Pennsylvania and had 4 more children. their names are\\nElizabeth C. Roth   Born   May 16 1982\\nGeorge J. Roth       Born   July 23 1884\\nMary L. Roth           Born   May 31 1887\\nJoseph G, Roth       Born   Feb 26 1893\\n\\nBy 1915 , The Roth Family had move to Pasadena Calif. \\nJoseph G. Roth would met Bertha R. Limacher, They dated for 4 years and on \\nMay 11 1919 got married. \\nFor the next few years they lived on the ranch with limacher family .\\n\\nBetween the years of 1920 and 1924 joe Roth had become a building contractor and built 2 homes next to each other at 2081 and 2075 Kirkwood Av, Pasadena . Around 1923 the Limacher’s had to sell the ranch to the city of Pasadena because of the new Development  Project to build the Rose Bowl and Golf Course, \\n\\nJoseph and Bertha moved into the house at 2081 and Mary and Jacob move into the house \\nat 2075 . The name of the street has sense been renamed Navarro Av.\\n --botresp-- Jacob Limacher was born in Luzern, Switzerland on February 20, 1858. He immigrated to New York in 1879 at the age of 21 and obtained his citizenship in 1905. \\n\\nMary Imfeld was born in Alpnach, Obwalden, Switzerland on May 19, 1856. She arrived in New York in 1882 at the age of 26 and also obtained her citizenship in 1905. \\n\\nJacob and Mary eventually met in Pasadena, California and got married in 1884. Jacob worked as a rancher and by 1900, they owned 45 acres of land in the Arroyo Saco in Pasadena, which they purchased for a ten dollar gold coin. They had a citrus farm, primarily consisting of orange trees, and by this time, they had four children. \\n\\nTheir oldest child, Walter, was 10 years old in 1900, followed by Louise (8 years old), George (5 years old), and Bertha Rose, also known as Berdie, who was 3 years old. \\n\\nAs Bertha grew older, she started working at The Flag Studio on East Colorado Street in Pasadena when she was around 16 or 17 years old. It was here that Bertha developed an interest in photography and purchased a Kodak Bellows camera, which she used throughout her life. \\n\\nAround 1913, Bertha met Joseph Roth, also known as Joe. Joseph was born on February 26, 1893 to George Roth and Elizabeth Deller. George and Elizabeth, along with their four children, immigrated to the United States from Germany in 1881. Unfortunately, George passed away during or shortly after the voyage. \\n\\nBy 1910, the Roth family was living in Pennsylvania and had four more children: Elizabeth C. Roth (born May 16, 1882), George J. Roth (born July 23, 1884), Mary L. Roth (born May 31, 1887), and Joseph G. Roth (born February 26, 1893). \\n\\nIn 1915, the Roth family relocated to Pasadena, California. Joseph met Bertha Limacher and they dated for four years before getting married on May 11, 1919. They initially lived on the Limacher family ranch before Joseph became a building contractor. Between 1920 and 1924, he built two neighboring homes at 2081 and 2075 Kirkwood Avenue in Pasadena, which is now Navarro Avenue. \\n\\nIn 1923, the Limacher family had to sell their ranch to the city of Pasadena due to development plans for the Rose Bowl and Golf Course. Joseph and Bertha moved into the house at 2081 Kirkwood Avenue, while Mary and Jacob moved into the adjacent house at 2075 Kirkwood Avenue.', clean_interaction='jacob limacher was born in luzern , switzerland on february 20 1858 . jacobs arrival to new york in 1879 . he is now 21 years old . citizenship in 1905 . mary imfeld was born in alpnach , obwalden , switzerland on may 19 1856 . mary arrival to new york in 1882 , she is now 26 years old . citizenship in 1905 . jacob spent some time in san francisco , but ended up in pasadena calif , as did mary imfeld . they meet and in 1884 they were marriedjacob limacher was a rancher and by 1900 we find them on 45 acres of land in the arroyo saco in pasadena calif . this piece of land according to the deed was purchased with a ten dollar gold coin . they had built a citrus farm of mostly orange trees and by now jacob and mary have 4 children . in 1900 walter the oldest is 10 years oldlouise is 8 years oldgeorge is 5 years oldbertha rose , the youngest is 3 yearsthe years go by and by the time bertha they called her berdie reached 16 or 17 years old , she started working at the flag studio on 26 east colorado street . flag studio sold kodak film , cameras and developed film . it was here thatbertha became interested in photography , bought a kodak bellows camera and started taking pictures , and do so the rest of her life . around this same time about 1913 , bertha met joseph roth . the started dating joseph george roth they called him joe was born february 26 1893 . his parents were george roth and elizabeth deller . george and elizabeth and their first 4 children immigrate to the us from germany in 1881 . they sailed from rotterdam , netherland to new york . the name of the ship they sailed on was the ss . p caland . the names of the four children that came with them arecatharine roth age 5 barbara roth age 4 franz or frank roth age 3 george age 1 george passed away on the voyage or soon afterby 1910 , the roth family were living in pennsylvania and had 4 more children . their names areelizabeth c . roth born may 16 1982 george j . roth born july 23 1884 mary l . roth born may 31 1887 joseph g , roth born feb 26 1893 by 1915 , the roth family had move to pasadena calif . joseph g . roth would met bertha r . limacher , they dated for 4 years and on may 11 1919 got married . for the next few years they lived on the ranch with limacher family . between the years of 1920 and 1924 joe roth had become a building contractor and built 2 homes next to each other at 2081 and 2075 kirkwood av , pasadena . around 1923 the limachers had to sell the ranch to the city of pasadena because of the new development project to build the rose bowl and golf course , joseph and bertha moved into the house at 2081 and mary and jacob move into the house at 2075 . the name of the street has sense been renamed navarro av . jacob limacher was born in luzern , switzerland on february 20 , 1858 . he immigrated to new york in 1879 at the age of 21 and obtained his citizenship in 1905 . mary imfeld was born in alpnach , obwalden , switzerland on may 19 , 1856 . she arrived in new york in 1882 at the age of 26 and also obtained her citizenship in 1905 . jacob and mary eventually met in pasadena , california and got married in 1884 . jacob worked as a rancher and by 1900 , they owned 45 acres of land in the arroyo saco in pasadena , which they purchased for a ten dollar gold coin . they had a citrus farm , primarily consisting of orange trees , and by this time , they had four children . their oldest child , walter , was 10 years old in 1900 , followed by louise 8 years old , george 5 years old , and bertha rose , also known as berdie , who was 3 years old . as bertha grew older , she started working at the flag studio on east colorado street in pasadena when she was around 16 or 17 years old . it was here that bertha developed an interest in photography and purchased a kodak bellows camera , which she used throughout her life . around 1913 , bertha met joseph roth , also known as joe . joseph was born on february 26 , 1893 to george roth and elizabeth deller . george and elizabeth , along with their four children , immigrated to the united states from germany in 1881 . unfortunately , george passed away during or shortly after the voyage . by 1910 , the roth family was living in pennsylvania and had four more children : elizabeth c . roth born may 16 , 1882 , george j . roth born july 23 , 1884 , mary l . roth born may 31 , 1887 , and joseph g . roth born february 26 , 1893 . in 1915 , the roth family relocated to pasadena , california . joseph met bertha limacher and they dated for four years before getting married on may 11 , 1919 . they initially lived on the limacher family ranch before joseph became a building contractor . between 1920 and 1924 , he built two neighboring homes at 2081 and 2075 kirkwood avenue in pasadena , which is now navarro avenue . in 1923 , the limacher family had to sell their ranch to the city of pasadena due to development plans for the rose bowl and golf course . joseph and bertha moved into the house at 2081 kirkwood avenue , while mary and jacob moved into the adjacent house at 2075 kirkwood avenue .', frequent_terms=['barbara', 'calif', 'dollar', 'called', '1913', 'unfortunately', 'city', 'old', 'started', '10', '23', 'louise', 'ss', 'eventually', 'working', 'jacob', 'spent', 'met', 'joseph', 'born', 'also', 'roth', '1924', 'joe', 'east', 'immigrate', 'youngest', 'bellows', 'arrived', 'got', 'next', '21', 'obtained', 'used', 'according', '1919', '31', '20', 'us', '1923', 'yearsthe', 'go', 'george', 'project', 'rancher', 'rest', 'saco', '19', 'ten', 'sold', 'contractor', 'parents', 'find', 'sell', '17', 'voyage', 'frank', 'consisting', 'flag', 'developed', 'farm', 'francisco', 'sailed', '1884', 'san', 'course', '1881', 'grew', '16', 'united', 'elizabeth', 'married', 'coin', 'oldest', '1879', 'bowl', '2081', '1893', 'due', 'july', 'four', 'getting', 'deller', 'ship', 'homes', 'away', 'av', '1858', 'soon', '1882', '1905', 'switzerland', 'pictures', 'cameras', 'film', 'avenue', 'years', 'orange', 'ranch', 'purchased', 'york', 'passed', 'came', 'pasadena', 'trees', 'reached', '1887', 'children', '1856', 'acres', 'bertha', 'states', 'owned', '26', 'primarily', 'interest', 'interested', 'renamed', 'older', 'piece', '45', 'worked', '1982', 'california', 'along', '1900', 'move', 'life', 'first', 'family', 'built', 'house', 'around', 'street', 'followed', 'adjacent', '1920', 'taking', 'throughout', 'navarro', 'become', 'dated', 'meet', 'build', 'building', 'germany', 'mary', 'names', 'arrival', 'relocated', 'citrus', 'land', 'jacobs', 'february', 'neighboring', 'dating', 'child', '11', 'name', 'rose', 'deed', 'franz', '2075', 'camera', 'time', 'plans', 'living', 'mostly', 'shortly', '1910', 'kodak', 'arroyo', 'became', 'pennsylvania', 'moved', 'rotterdam', 'known', 'colorado', 'walter', 'citizenship', 'feb', 'studio', 'gold', 'kirkwood', 'immigrated', 'initially', 'new', 'golf', 'lived', 'photography', 'netherland', 'ended', 'age', '1915', 'may', 'bought', 'development'])"
      ]
     },
     "execution_count": 46,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "results.first()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6f886d02-8a4f-4e20-b304-c0fa286d721b",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "markdown",
   "id": "f3bcfe87-379d-496a-8975-52c33c91354d",
   "metadata": {},
   "source": [
    "#Testing"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c58b542d-abdc-42a5-ac22-735423114e6f",
   "metadata": {},
   "outputs": [],
   "source": [
    "#classifier = pipeline(\"zero-shot-classification\", model=\"facebook/bart-large-mnli\", device='mps')\n",
    "#labels = ['code', 'plain text']"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "5b0f1160-c0f4-420c-afe1-ca49402af65e",
   "metadata": {},
   "outputs": [],
   "source": [
    "'''sc = SparkContext.getOrCreate()\n",
    "classifier_broadcast = sc.broadcast((classifier, labels))\n",
    "\n",
    "def textClassifier(prompt):\n",
    "    classifier, labels = classifier_broadcast.value\n",
    "    result = classifier(prompt, labels)\n",
    "    return result[\"labels\"][0]\n",
    "\n",
    "classify_udf = F.udf(textClassifier, StringType())\n",
    "process_data.withColumn('prompt-type', classify_udf(F.col('clean'))).distinct().persist().collect() '''\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "fff6fdbb-3258-4550-b11a-10ff7d89731f",
   "metadata": {},
   "outputs": [],
   "source": [
    "'''# Function to classify a batch of prompts\n",
    "def classify_batch(prompts):\n",
    "    results = []\n",
    "    for prompt in prompts:\n",
    "        try:\n",
    "            result = classifier(prompt, labels)\n",
    "            results.append(result['labels'][0])  # Add the top label\n",
    "        except Exception as e:\n",
    "            results.append(\"error\")  # Handle any classification errors\n",
    "    return results'''"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c054a2f1-da93-466c-8c0c-50d5422e42e1",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "#clean_prompts = process_data.select('clean').toPandas()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "77f98f08-92a6-4f3a-a2a6-36f65da1fa8e",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b56a4af5-8594-4648-80d8-2a336a611454",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "'''from transformers import pipeline\n",
    "\n",
    "# Load the zero-shot classification pipeline\n",
    "classifier = pipeline(\"zero-shot-classification\", model=\"facebook/bart-large-mnli\", device = 'mps')\n",
    "\n",
    "# Define the input text\n",
    "input_text = 'Hey there! Are you familiar with reality shifting? So, I’m refining a foolproof method for reality shifting and want to pick a destination. Want to help me? I’m thinking something pretty personalized. There are a few things that are required of my destination. 1. The quest. I have to have a clear overarching goal in my reality, and don’t make it too crazy. It should be more along the lines of “save the president’s daughter” or “escape this weird wacky sinister place” NOT “get an artifact that literally controls reality”. Seriously, don’t make me fetch an artifact, or fetch anything. Instead, make me DO something. 2. Babes. I need pretty girls. 3. The entry. I need to get to lose consciousness in order to begin my journey in my desired reality, preferably by having it knocked out by one of the aforementioned babes. 4. Action. It needs to be cool. 5. Unconsciousness. Myself and the babes need to pass out in this place, preferably by being knocked out in some way or fainting. And it should happen, like, a lot. With these requirements in mind, you got any unique refined ideas? Don’t be vague, be extremely specific. Also, make your response as long and detailed as possible. Be super specific, especially when describing the world. The world should be self-contained and relatively small/understandable. Also, try to be conversational. Describe the world well.'\n",
    "\n",
    "\n",
    "# Define possible labels for classification\n",
    "labels = [\"code\", \"plain text\"]\n",
    "\n",
    "# Perform classification\n",
    "result = classifier(input_text, labels)\n",
    "\n",
    "# Print the classification result\n",
    "print(result)\n",
    "print(\"Classification:\", result[\"labels\"][0])  # The most likely label'''"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "64436c64-327e-4d51-a6cf-1ca653864b35",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b6440a9d-3341-4070-83d3-b83cb4dfb56e",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e34a1d99-e268-4799-a1cb-47406c9239ae",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3a1278f6-654d-450e-bc78-6bfca6863a2c",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a0807d63-d36b-4142-9a8f-69e365dddf82",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "951a26b5-a0eb-4dcc-bb84-923384563617",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "02bc8bd1-d0a8-4216-8d13-14dba8b2f4c3",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}