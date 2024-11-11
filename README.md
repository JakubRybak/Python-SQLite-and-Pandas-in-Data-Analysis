StackExchange Travel Analysis 2023/2024
Overview
This project, "StackExchange Travel Analysis 2023/2024," is part of an academic assignment (Homework 3) for the PDU course. The main objective is to analyze a simplified, anonymized dataset from the StackExchange Travel platform, applying SQL and Pandas methods to explore, process, and extract insights from data related to users, posts, comments, links, and votes.

The project demonstrates proficiency in using the SQLite database and Pandas library to query and manipulate data, with each SQL query solution paired with an equivalent Pandas implementation. The data is processed using Python scripts, exported to an SQLite database, and queried for meaningful insights.

Dataset Description
The project uses an anonymized dataset from StackExchange Travel, containing the following data frames:

Posts.csv.gz: Information on user posts.
Users.csv.gz: Details on users and their profiles.
Comments.csv.gz: Comments on posts made by users.
PostLinks.csv.gz: Connections between different posts.
Votes.csv.gz: Voting data on posts and comments.
This dataset is available as a compressed .csv file and is imported and analyzed in Python.

Project Structure
Tasks
The project consists of five main tasks, each involving data manipulation and SQL query writing. The tasks are as follows:

Monthly User Account Summary: Calculate total accounts created each month and the average reputation for each.
Top Active Users by Comments: Identify the top 10 users by the total number of comments on answers they have authored.
Spam Detection: List posts identified as spam and display relevant user data for each spam post.
User Duplicate Question Count: List users with more than 100 duplicate questions posted, along with upvote, downvote, and reputation statistics.
Most Frequent Question Duplicates by Daytime: Analyze the frequency of duplicate questions posted and categorize the posting time by daytime (e.g., Night, Morning, Day, Evening).
Each task is implemented using two approaches:

SQL Queries: SQL statements run via pandas.read_sql_query().
Pandas Methods: Direct data manipulation using Pandas methods, ensuring consistent results across both methods.

Files
travel_stackexchange_com/: Directory containing the source CSV files (Posts.csv.gz, Users.csv.gz, Comments.csv.gz, PostLinks.csv.gz, Votes.csv.gz).
Jakub_Rybak_333156_PD3.py: Main script containing the SQL queries and Pandas methods for each task.
README.md: This documentation file.
