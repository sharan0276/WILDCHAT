# WILDCHAT
 <br>IDMP Project Fall 2024

 <br>This project focusses on the WildCHAT datatset, which is a corpus of 1M chat interactions from anonymized users and chat gpt bots. The dataset is studied, to implement a keyword based search to find similar chat responses based on filter catgeories ( keyword, country, state , bot model ). Further the similar chat interactions are summarized to get a better understanding of the overall interaction from that particular user. A sample of the data set for this project is available in the GIT,  
 <br> <br> <br>
 
##PREPROCESSING

 <br> &nbsp; 1. The primary focus on conversations in this project will be the interactions in English language.</br>

 <br> &nbsp; 2. All toxic data is exceluded from the project scope.</br>

 <br> &nbsp; 3. The dataset contains the following schema : </br>
       <br>root</br>
       <br>|-- conversation_hash: string (nullable = true)</br>
       <br>|-- model: string (nullable = true)</br>
       <br>|-- timestamp: timestamp (nullable = true)</br>
       <br>|-- conversation: array (nullable = true)</br>
       <br>|    |-- element: struct (containsNull = true)</br>
       <br>|    |    |-- content: string (nullable = true)</br>
       <br>|    |    |-- country: string (nullable = true)</br>
       <br>|    |    |-- hashed_ip: string (nullable = true)</br>
       <br>|    |    |-- header: struct (nullable = true)</br>
       <br>|    |    |    |-- accept-language: string (nullable = true)</br>
       <br>|    |    |    |-- user-agent: string (nullable = true)</br>
       <br>|    |    |-- language: string (nullable = true)</br>
       <br>|    |    |-- redacted: boolean (nullable = true)</br>
       <br>|    |    |-- role: string (nullable = true)</br>
       <br>|    |    |-- state: string (nullable = true)</br>
       <br>|    |    |-- timestamp: timestamp (nullable = true)</br>
       <br>|    |    |-- toxic: boolean (nullable = true)</br>
       <br>|    |    |-- turn_identifier: long (nullable = true)</br>
       <br>|-- turn: long (nullable = true)</br>
       <br>|-- language: string (nullable = true)</br>
       <br>|-- openai_moderation: array (nullable = true)</br>
       <br>|    |-- element: struct (containsNull = true)</br>
       <br>|    |    |-- categories: struct (nullable = true)</br>
       <br>|    |    |    |-- harassment: boolean (nullable = true)</br>
       <br>|    |    |    |-- harassment/threatening: boolean (nullable = true)</br>
       <br>|    |    |    |-- harassment_threatening: boolean (nullable = true)</br>
       <br>|    |    |    |-- hate: boolean (nullable = true)</br>
       <br>|    |    |    |-- hate/threatening: boolean (nullable = true)</br>
       <br>|    |    |    |-- hate_threatening: boolean (nullable = true)</br>
       <br>|    |    |    |-- self-harm: boolean (nullable = true)</br>
       <br>|    |    |    |-- self-harm/instructions: boolean (nullable = true)</br>
       <br>|    |    |    |-- self-harm/intent: boolean (nullable = true)</br>
       <br>|    |    |    |-- self_harm: boolean (nullable = true)</br>
       <br>|    |    |    |-- self_harm_instructions: boolean (nullable = true)</br>
       <br>|    |    |    |-- self_harm_intent: boolean (nullable = true)</br>
       <br>|    |    |    |-- sexual: boolean (nullable = true)</br>
       <br>|    |    |    |-- sexual/minors: boolean (nullable = true)</br>
       <br>|    |    |    |-- sexual_minors: boolean (nullable = true)</br>
       <br>|    |    |    |-- violence: boolean (nullable = true)</br>
       <br>|    |    |    |-- violence/graphic: boolean (nullable = true)</br>
       <br>|    |    |    |-- violence_graphic: boolean (nullable = true)</br>
       <br>|    |    |-- category_scores: struct (nullable = true)</br>
       <br>|    |    |    |-- harassment: double (nullable = true)</br>
       <br>|    |    |    |-- harassment/threatening: double (nullable = true)</br>
       <br>|    |    |    |-- harassment_threatening: double (nullable = true)</br>
       <br>|    |    |    |-- hate: double (nullable = true)</br>
       <br>|    |    |    |-- hate/threatening: double (nullable = true)</br>
       <br>|    |    |    |-- hate_threatening: double (nullable = true)</br>
       <br>|    |    |    |-- self-harm: double (nullable = true)</br>
       <br>|    |    |    |-- self-harm/instructions: double (nullable = true)</br>
       <br>|    |    |    |-- self-harm/intent: double (nullable = true)</br>
       <br>|    |    |    |-- self_harm: double (nullable = true)</br>
       <br>|    |    |    |-- self_harm_instructions: double (nullable = true)</br>
       <br>|    |    |    |-- self_harm_intent: double (nullable = true)</br>
       <br>|    |    |    |-- sexual: double (nullable = true)</br>
       <br>|    |    |    |-- sexual/minors: double (nullable = true)</br>
       <br>|    |    |    |-- sexual_minors: double (nullable = true)</br>
       <br>|    |    |    |-- violence: double (nullable = true)</br>
       <br>|    |    |    |-- violence/graphic: double (nullable = true)</br>
       <br>|    |    |    |-- violence_graphic: double (nullable = true)</br>
       <br>|    |    |-- flagged: boolean (nullable = true)</br>
       <br>|-- detoxify_moderation: array (nullable = true)</br>
       <br>|    |-- element: struct (containsNull = true)</br>
       <br>|    |    |-- identity_attack: double (nullable = true)</br>
       <br>|    |    |-- insult: double (nullable = true)</br>
       <br>|    |    |-- obscene: double (nullable = true)</br>
       <br>|    |    |-- severe_toxicity: double (nullable = true)</br>
       <br>|    |    |-- sexual_explicit: double (nullable = true)</br>
       <br>|    |    |-- threat: double (nullable = true)</br>
       <br>|    |    |-- toxicity: double (nullable = true)</br>
       <br>|-- toxic: boolean (nullable = true)</br>
       <br>|-- redacted: boolean (nullable = true)</br>
       <br>|-- state: string (nullable = true)</br>
       <br>|-- country: string (nullable = true)</br>
       <br>|-- hashed_ip: string (nullable = true)</br>
       <br>|-- header: struct (nullable = true)</br>
       <br>|    |-- accept-language: string (nullable = true)</br>
       <br>|    |-- user-agent: string (nullable = true)</br>
   Of which, Header, detoxify_moderation, and openai_moderation fields are dropped to avoid using any toxic data set and to preserve a clarity in the dataset.

4. To better understand the user / bot interactions , the conversation field is explored.
       conversation: array (nullable = true)
       |    |-- element: struct (containsNull = true)
       |    |    |-- content: string (nullable = true)
       |    |    |-- country: string (nullable = true)
       |    |    |-- hashed_ip: string (nullable = true)
       |    |    |-- header: struct (nullable = true)
       |    |    |    |-- accept-language: string (nullable = true)
       |    |    |    |-- user-agent: string (nullable = true)
       |    |    |-- language: string (nullable = true)
       |    |    |-- redacted: boolean (nullable = true)
       |    |    |-- role: string (nullable = true)
       |    |    |-- state: string (nullable = true)
       |    |    |-- timestamp: timestamp (nullable = true)
       |    |    |-- toxic: boolean (nullable = true)
       |    |    |-- turn_identifier: long (nullable = true)
