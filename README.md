# WILDCHAT
 <br>IDMP Project Fall 2024

 <br>This project focusses on the WildCHAT datatset, which is a corpus of 1M chat interactions from anonymized users and chat gpt bots. The dataset is studied, to implement a keyword based search to find similar chat responses based on filter catgeories ( keyword, country, state , bot model ). Further the similar chat interactions are summarized to get a better understanding of the overall interaction from that particular user. A sample of the data set for this project is available in the GIT,  
 <br> <br> <br>
 
## PREPROCESSING

 <br> &nbsp; 1. The primary focus on conversations in this project will be the interactions in English language.</br>

 <br> &nbsp; 2. All toxic data is exceluded from the project scope.</br>

 <br> &nbsp; 3. The dataset contains the following schema : </br>
       <br>&ensp;root</br>
       <br>&ensp;&nbsp;|-- conversation_hash: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- model: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- timestamp: timestamp (nullable = true)</br>
       <br>&ensp;&nbsp;|-- conversation: array (nullable = true)</br>
       <br>&ensp;&ensp;|    |-- element: struct (containsNull = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- content: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- country: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- hashed_ip: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- header: struct (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- accept-language: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- user-agent: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- language: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- redacted: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- role: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- state: string (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- timestamp: timestamp (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- toxic: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- turn_identifier: long (nullable = true)</br>
       <br>&ensp;&nbsp;|-- turn: long (nullable = true)</br>
       <br>&ensp;&nbsp;|-- language: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- openai_moderation: array (nullable = true)</br>
       <br>&ensp;&ensp;|    |-- element: struct (containsNull = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- categories: struct (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment/threatening: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment_threatening: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate/threatening: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate_threatening: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm/instructions: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm/intent: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm_instructions: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm_intent: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual/minors: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual_minors: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence/graphic: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence_graphic: boolean (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- category_scores: struct (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment/threatening: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- harassment_threatening: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate/threatening: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- hate_threatening: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm/instructions: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self-harm/intent: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm_instructions: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- self_harm_intent: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual/minors: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- sexual_minors: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence/graphic: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |    |-- violence_graphic: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- flagged: boolean (nullable = true)</br>
       <br>&ensp;&nbsp;|-- detoxify_moderation: array (nullable = true)</br>
       <br>&ensp;&ensp;|    |-- element: struct (containsNull = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- identity_attack: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- insult: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- obscene: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- severe_toxicity: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- sexual_explicit: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- threat: double (nullable = true)</br>
       <br>&ensp;&ensp;&nbsp;|    |    |-- toxicity: double (nullable = true)</br>
       <br>&ensp;&nbsp;|-- toxic: boolean (nullable = true)</br>
       <br>&ensp;&nbsp;|-- redacted: boolean (nullable = true)</br>
       <br>&ensp;&nbsp;|-- state: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- country: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- hashed_ip: string (nullable = true)</br>
       <br>&ensp;&nbsp;|-- header: struct (nullable = true)</br>
       <br>&ensp;&ensp;|    |-- accept-language: string (nullable = true)</br>
       <br>&ensp;&ensp;|    |-- user-agent: string (nullable = true)</br>
       <br><br>
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
