# Game-Recommendation-System-Design

## Introduction
Using Steam user data, this repository manages to recommend 10 games for each Steam users based on knowledge based, content based and item based collaborative filtering algorithm, and build a web app to visualize it.

## Workflow
1. Scraped 5000 Steam user game inventory and 70000 game details by Python web crawler from Steam Game Platform via API request
2. Performed ETL manipulation to extracted numerical and categorical features from JSON data, transformed them into pandas Dataframe and stored it in MySQL database 
3. Achieved 10 new games recommendation for each user based on knowledge-based, content-based, and item-based collaborative filtering recommendation algorithm via Python and Spark
4. Developed a web application to display the product using Flask

## File Description
- scrape_data.py uses steam web API to scrape game details and user inventories
- recommendation.py provides the whole code doing ETL, knowledge-based recommendation, content-based recommendation and collaborative filtering recommendation
- the website folder contains all codes needed to run the web application. To use this web application, you can download the repository and run the run.py in website folder

## Installation
To replicate the findings and execute the code in this repository you will need basically the following Python packages:
- Numpy
- pandas
- scikit-learn
- requests
- bs4
- sqlalchemy
- pyspark

## Resource
Data Application Lab All rights reserved.
