# fbref-scraper
A multithreaded web scraper that collects all player data from fbref.com for the top 5 European soccer leagues and stores it in a MySQL database. 

## Init
Create a .env file and place it under the "src/scraper" folder.  
You have to define the following env variables:
- DATABASE: the name of the DB you are connecting to
- DB_HOST: the host number (i.e. 127.0.0.1, localhost...)
- DB_USER: the username for DB connection
- DB_PSW: the password for DB connection
- LOG_FILE: tha name of the log file

For testing purposes, you may create a new .env.test file under 
the "src/test" folder.

## Crawler/Scraper
A multithreaded web scraper using BeautifulSoup. Iteratively crawls through teams from the top 5 European soccer leagues and scrapes the player performance data for their players.
<br>Sample run with 8 worker processes:

<p align="center">
  <img src="https://user-images.githubusercontent.com/66108163/147793493-b4fffde7-1633-43c9-9e85-b72403aff9a8.gif" alt="animated" />
</p>

## Database
A MySQL database modeled after the format of tables from fbref. PyMySQL is used to connect to and query the database. 
<br>A look at the database and a sample query. Select all players who have averaged more than 15 goals per season. No surprises here...

<p align="center">
  <img src="https://user-images.githubusercontent.com/66108163/147796537-e6e0c159-842a-4ea4-afd0-f74f5d653994.gif" alt="animated" />
</p>

## Dataset
You can find the final dataset here: https://www.kaggle.com/biniyamyohannes/soccer-player-data-from-fbrefcom
