# tensoriot

1. main function: - Initializes a spark session.
                  - reads the raw metadata and reviews compressed json files into dataframes.
                  - Cleans both the dataframes by dropping unnessesary columns and caches the dataframes because it is being used in the following functions.
                  
2. max_min_reviews(): - It creates a new dataframe which contains count of reviews for each asin.
                    - It then joins with the meta df to get the title of the item.
                    - max_review_count and min_review_count contains the maximum and minimum number of reviews along with asin and title of the product.
                    
3. longest_review(): - It creates a new dataframe with the length of reviews for each asin
                   - max_review_length contains the asin, title and the maximum length of review among all the product's reviews.   

4. convert_date(): - It creates a new dataframe which uses the unixtimestamp(already converted to readable timestamp in main funtion) and converts it to MM-dd-yyyy format     

5. multiple_reviews() - It creates a new dataframe and utilized window funtions to find if there are multiple reviews done by a single reviewer.

