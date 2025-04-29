# WILDCHAT
 <br>IDMP Project - Fall 2024

 <br>This project focuses on the WildCHAT datatset, which is a corpus of 1 million chat interactions from anonymized users and chatGPT conversation bots. The dataset is processed, wrangled and investigated, in part to implement a keyword based search to find similar chat responses based on filter catgeories ( keyword, country, state , bot model ). Further, the similar chat interactions are summarized to get a better understanding of the overall interaction from that particular user. A sample of the data set for this project is available in a github subfolder and the dataset and results are available upon request. No potentially dangerous or personally identifiable information has been published on this public facing repository.  
 <br> <br> <br>
 
## PREPROCESSING

 <br> &nbsp; 1. The primary focus on conversations in this project will be the interactions in English language.</br>

 <br> &nbsp; 2. All toxic and non redacted conversations are excluded from the keyword search.</br>

 <br> &nbsp; 3. To better capture the interaction that exists between the user and chatbot, the conversation field in the dataset is exploded and the individual interactions are grouped together based on turn identifier to extract the context better. </br>

 <br> &nbsp; 4. A separator ' --botresp-- ' is added between the user prompt and the bot response for better readability. </br>
 
 <br> &nbsp; 5. The combined interaction is cleaned, and the words that appear more frequently in the interaction are saved in the 'frequent words'. field. </br>

## ANALYSIS TECHNIQUES

<br> A preprocessing pipeline was developed using PySpark to enhance keyword extraction, employing tokenization, stop word removal, count vectorization (excluding rare or overly common words), TF-IDF transformation, and feature scaling. A frequent-terms column was created, containing tokens with normalized TF-IDF values >0.20, ensuring efficient keyword extraction and noise reduction.</br>

<br> An interactive Flask application was developed to enable powerful keyword search and summarization capabilities. Users can query the dataset by keyword, country, state, and GPT model, gaining real-time insights into over 1 million ChatGPT interactions.</br>

<br>The project leverages PySpark for preprocessing and the BART model for summarization, integrated into a Flask-based interface for keyword search and filtering. Key findings highlight instances of ChatGPT misuse in professional journalism, as well as comparisons of coding responses between ChatGPT and a specialized coding model, DeepSeek. Please read our final project report which discusses related scientific literature, more results and conclusions as well as ethical implications in journalism and programming: [https://github.com/sharan0276/WILDCHAT/blob/main/Final_Project_Report.pdf](url).</br>
